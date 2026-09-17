using K4os.Compression.LZ4;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.RateLimiting;
using System.Threading.Tasks;
using Shared.Common;
using Shared.Extensions;
using Shared.Stock.SQL;
using Stock = Shared.Stock;
using Crypto = Shared.Crypto;

namespace PolygonMarketData.API
{
	public sealed class Service : IDisposable
	{
		public const string NAME = "Polygon";
		private readonly IServiceScopeFactory _scopeFactory;
		private readonly IHttpClientFactory _httpClientFactory;
		private readonly ILogger _logger;
		private readonly Settings _settings;
		private readonly TokenBucketRateLimiter _HTTPRateLimiter;
		
		public Service( IOptions<Settings> tSettings, IServiceScopeFactory tScopeFactory, IHttpClientFactory tHttpClientFactory, ILoggerFactory tLoggerFactory ) : base()
		{
			_scopeFactory = tScopeFactory;
			_httpClientFactory = tHttpClientFactory;
			_logger = tLoggerFactory.CreateLogger<Service>();
			_settings = tSettings.Value;
			_HTTPRateLimiter = new
			(
				new()
				{
					TokenLimit = 1,
					TokensPerPeriod = 1,
					ReplenishmentPeriod = TimeSpan.FromMilliseconds( 11 ),
					AutoReplenishment = true,
					QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
					QueueLimit = int.MaxValue
				}
			); // <=100 requests per second
		}

		public void Dispose()
		{
			_HTTPRateLimiter.Dispose();
		}

		private DateOnly ClampStart( DateOnly tStart )
		{
			DateOnly tempMinStart = new( 2003, 10, 1 );

			return tStart < tempMinStart ? tempMinStart : tStart;
		}

		private DateOnly ClampOptionKlineStart( DateOnly tStart )
		{
			DateOnly tempMinStart = new( 2014, 7, 1 );

			return tStart < tempMinStart ? tempMinStart : tStart;
		}

		public async Task<ConcurrentDictionary<DateOnly, Dictionary<DateTime, Stock.Kline>>> GetKlinesAsync( HolidayFactory tHolidayFactory, string tTicker, DateOnly tStart, DateOnly tEnd, int tSkipLastBeforeClose, ParallelOptions tParallelOptions )
		{
			tParallelOptions.CancellationToken.ThrowIfCancellationRequested();

			// Get cached SQL data
			tStart = ClampStart( tStart );
			Dictionary<DateOnly, Holiday> tempStockHolidays = tHolidayFactory.GetStockMarketHolidays( tStart.Year, tEnd.Year );
			Dictionary<DateOnly, Stock.SQL.Kline> tempKlinesDictionary = [];
			Ticker tempTicker;
			List<Stock.SQL.Kline> tempSQLKlines;

			using ( IServiceScope tempScope = _scopeFactory.CreateScope() )
			{
				Stock.SQL.Context tempContext = tempScope.ServiceProvider.GetRequiredService<Stock.SQL.Context>();
				tempTicker = await tempContext.Ticker.Where( x => x.Name == tTicker ).FirstOrDefaultAsync( tParallelOptions.CancellationToken );

				if ( tempTicker == null )
				{
					tempTicker = new() { Name = tTicker };
					await tempContext.Ticker.AddAsync( tempTicker, tParallelOptions.CancellationToken );
					await tempContext.SaveChangesAsync( tParallelOptions.CancellationToken );
				}

				tempSQLKlines = await tempContext.Kline.AsNoTracking().Where( x => x.TickerId == tempTicker.TickerId && x.SkipLastBeforeClose == tSkipLastBeforeClose && x.Time >= tStart && x.Time <= tEnd ).ToListAsync( tParallelOptions.CancellationToken );
			}

			foreach ( Stock.SQL.Kline tempKline in tempSQLKlines )
			{
				tempKlinesDictionary.TryAdd( tempKline.Time, tempKline );
			}

			tempSQLKlines.Clear();

			// Identify missing data
			List<KlineRequest> tempMissing = [];

			for ( DateOnly tempDate = tStart; tempDate <= tEnd; tempDate = tempDate.AddDays( 1 ) )
			{
				if ( tempDate.DayOfWeek != DayOfWeek.Saturday && tempDate.DayOfWeek != DayOfWeek.Sunday
					&& ( !tempStockHolidays.TryGetValue( tempDate, out Holiday tempHoliday ) || tempHoliday.Close.HasValue )
					&& !tempKlinesDictionary.ContainsKey( tempDate )
				)
				{
					bool tempIsDST = tHolidayFactory.GetIsDaylightSavingTime( new DateTime( tempDate, new TimeOnly( 12, 0 ), DateTimeKind.Utc ) );

					TimeOnly tempMarketOpen = tHolidayFactory.GetStockMarketOpenTime( tempIsDST );
					TimeOnly tempMarketClose = tempHoliday != null && tempHoliday.Close.HasValue ? tempHoliday.Close.Value : tHolidayFactory.GetStockMarketCloseTime( tempIsDST );

					tempMissing.Add
					(
						new
						(
							new DateTime( tempDate, tempMarketOpen, DateTimeKind.Utc ),
							new DateTime( tempDate, tempMarketClose, DateTimeKind.Utc ),
							tSkipLastBeforeClose
						)
					);
				}
			}

			// Get and match missing data and save to database
			List<Task> tempInsertTasks = [];
			List<Stock.SQL.Kline> tempBatchedKlines = [];

			await foreach ( Stock.SQL.Kline tempKline in GetKlinesAsync( tempTicker, tempMissing, tParallelOptions.CancellationToken ) )
			{
				tempBatchedKlines.Add( tempKline );

				while ( tempBatchedKlines.Count >= _settings.KlineBatchSize )
				{
					Stock.SQL.Kline[] tempBatch = [ .. tempBatchedKlines.Take( _settings.KlineBatchSize ) ];
					tempBatchedKlines.RemoveRange( 0, _settings.KlineBatchSize );
					tempInsertTasks.Add( InsertBatchAsync( tempBatch, tParallelOptions.CancellationToken ) );
				}

				tempKlinesDictionary[ tempKline.Time ] = tempKline;
			}

			while ( tempBatchedKlines.Count > 0 )
			{
				int tempRemaining = Math.Min( _settings.KlineBatchSize, tempBatchedKlines.Count );
				Stock.SQL.Kline[] tempBatch = [ .. tempBatchedKlines.Take( tempRemaining ) ];
				tempBatchedKlines.RemoveRange( 0, tempRemaining );
				tempInsertTasks.Add( InsertBatchAsync( tempBatch, tParallelOptions.CancellationToken ) );
			}

			await Task.WhenAll( tempInsertTasks );

			// Output
			ConcurrentDictionary<DateOnly, Dictionary<DateTime, Stock.Kline>> tempDeserialized = [];

			await Parallel.ForEachAsync
			(
				tempKlinesDictionary,
				tParallelOptions,
				( x, tCancel ) =>
				{
					tCancel.ThrowIfCancellationRequested();

					try
					{
						if ( x.Value?.Data != null )
						{
							byte[] tempData = GetDecompressed( x.Value.Data );
							using MemoryStream tempStream = new( tempData );
							using BinaryReader tempReader = new( tempStream );

							int tempKlinesLength = tempReader.ReadInt32();
							Dictionary<DateTime, Stock.Kline> tempKlines = new( tempKlinesLength );

							for ( int i = 0; i < tempKlinesLength; ++i )
							{
								Stock.Kline tempKline = Stock.Kline.Deserialize( tempReader );
								tempKlines[ tempKline.Time ] = tempKline;
							}

							tempDeserialized.TryAdd( x.Key, tempKlines );
						}
					}
					catch ( Exception tException )
					{
						_logger.LogError( tException, "Error deserializing Klines for {Date}", x.Key );

						throw;
					}

					return ValueTask.CompletedTask;
				}
			);

			return tempDeserialized;
		}

		private async Task InsertBatchAsync( Stock.SQL.Kline[] tBatch, CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			DateOnly tempToday = DateOnly.FromDateTime( DateTime.UtcNow );
			List<Stock.SQL.Kline> tempBatch = [ .. tBatch.Where( x => x.Time < tempToday ) ];

			if ( tempBatch.Count > 0 )
			{
				using IServiceScope tempScope = _scopeFactory.CreateScope();
				Stock.SQL.Context tempContext = tempScope.ServiceProvider.GetRequiredService<Stock.SQL.Context>();
				await tempContext.Kline.AddRangeAsync( tempBatch, tCancel );
				await tempContext.SaveChangesAsync( tCancel );
			}
		}

		public readonly record struct KlineRequest( DateTime MarketOpen, DateTime MarketClose, int SkipLastBeforeClose );

		public async IAsyncEnumerable<Stock.SQL.Kline> GetKlinesAsync( Ticker tTicker, IReadOnlyCollection<KlineRequest> tRequests, [EnumeratorCancellation] CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			using CancellationTokenSource tempCancelSource = CancellationTokenSource.CreateLinkedTokenSource( tCancel );
			CancellationToken tempCancel = tempCancelSource.Token;
			Task<Stock.SQL.Kline>[] tempTasks = [ .. tRequests.Select( x => GetKlineAsync( tTicker, x, tempCancel ) ) ];

			try
			{
				await foreach ( Task<Stock.SQL.Kline> tempTask in Task.WhenEach( tempTasks ).WithCancellation( tempCancel ) )
				{
					yield return await tempTask;
				}
			}
			finally
			{
				tempCancelSource.Cancel();

				try
				{
					await Task.WhenAll( tempTasks );
				}
				catch { }
			}
		}

		private async Task<Stock.SQL.Kline> GetKlineAsync( Ticker tTicker, KlineRequest tRequest, CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			DateTime tempEnd = tRequest.MarketClose.AddMinutes( -( tRequest.SkipLastBeforeClose + 1 ) ); // inclusive
			DateOnly tempDate = DateOnly.FromDateTime( tRequest.MarketClose );
			List<Stock.Kline> tempKlines = [];

			try
			{
				long tempStartMilliseconds = new DateTimeOffset( tRequest.MarketOpen ).ToUnixTimeMilliseconds();
				long tempEndMilliseconds = new DateTimeOffset( tempEnd ).ToUnixTimeMilliseconds();
				string tempTicker = Uri.EscapeDataString( tTicker.Name );
				string tempURL = $"{_settings.URI}/v2/aggs/ticker/{tempTicker}/range/1/minute/{tempStartMilliseconds}/{tempEndMilliseconds}?adjusted=false&sort=asc&limit=50000";

				// Get API data
				do
				{
					using HttpResponseMessage tempResponse = await GetAsync( tempURL, tCancel );
					await using Stream tempStream = await tempResponse.Content.ReadAsStreamAsync( tCancel );

					KlineResponse tempRoot = await JsonSerializer.DeserializeAsync<KlineResponse>(tempStream, cancellationToken: tCancel) ?? throw new JsonException($"Get Klines Error: Empty response: {tRequest.MarketOpen}-{tempEnd}");

					if ( tempRoot.Status != "OK" )
					{
						throw new InvalidDataException( $"Get Klines API Error: {tempRoot.Status}: {tRequest.MarketOpen}-{tempEnd}" );
					}
					else if ( tempRoot.Results != null )
					{
						foreach ( KlineResult tempKline in tempRoot.Results )
						{
							DateTime tempTime = DateTimeOffset.FromUnixTimeMilliseconds( tempKline.Time ).UtcDateTime;

							tempKlines.Add
							(
								new
								(
									tempTime,
									tempKline.Open,
									tempKline.High,
									tempKline.Low,
									tempKline.Close,
									decimal.ToInt64( decimal.Truncate( tempKline.Volume ) )
								)
							);
						}
					}

					tempURL = string.IsNullOrWhiteSpace( tempRoot.NextURL ) ? null : tempRoot.NextURL;
				}
				while ( tempURL != null );

				// Serialize and compress
				Stock.SQL.Kline tempSQLKline = new()
				{
					TickerId = tTicker.TickerId,
					Time = tempDate,
					SkipLastBeforeClose = tRequest.SkipLastBeforeClose,
				};

				using MemoryStream tempData = new();
				using BinaryWriter tempWriter = new( tempData );

				tempWriter.Write( tempKlines.Count );

				foreach ( Stock.Kline tempKline in tempKlines )
				{
					tempKline.Serialize( tempWriter );
				}

				tempWriter.Flush();
				tempSQLKline.Data = GetCompressed( tempData.ToArray() );

				return tempSQLKline;
			}
			catch ( OperationCanceledException ) when ( tCancel.IsCancellationRequested )
			{
				throw;
			}
			catch ( Exception tException )
			{
				_logger.LogError( tException, "Get Klines Error: {Date}", tempDate );

				throw;
			}
		}

		public async Task<Dictionary<DateOnly, Stock.DailyKline>> GetDailyKlinesAsync( HolidayFactory tHolidayFactory, string tTicker, DateOnly tStart, DateOnly tEnd, CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			// Get cached SQL data
			tStart = ClampStart( tStart );
			Dictionary<DateOnly, Holiday> tempStockHolidays = tHolidayFactory.GetStockMarketHolidays( tStart.Year, tEnd.Year );
			Dictionary<DateOnly, Stock.SQL.DailyKline> tempKlinesDictionary = [];
			Ticker tempTicker;
			List<Stock.SQL.DailyKline> tempSQLKlines;

			using ( IServiceScope tempScope = _scopeFactory.CreateScope() )
			{
				Stock.SQL.Context tempContext = tempScope.ServiceProvider.GetRequiredService<Stock.SQL.Context>();
				tempTicker = await tempContext.Ticker.Where( x => x.Name == tTicker ).FirstOrDefaultAsync( tCancel );

				if ( tempTicker == null )
				{
					tempTicker = new() { Name = tTicker };
					await tempContext.Ticker.AddAsync( tempTicker, tCancel );
					await tempContext.SaveChangesAsync( tCancel );
				}

				tempSQLKlines = await tempContext.DailyKline.AsNoTracking().Where( x => x.TickerId == tempTicker.TickerId && x.Time >= tStart && x.Time <= tEnd ).ToListAsync( tCancel );
			}

			foreach ( Stock.SQL.DailyKline tempKline in tempSQLKlines )
			{
				tempKlinesDictionary.TryAdd( tempKline.Time, tempKline );
			}

			tempSQLKlines.Clear();

			// Identify missing data
			List<Range<DateOnly>> tempMissing = [];
			DateOnly? tempMissingStart = null;
			DateOnly? tempMissingEnd = null;

			for ( DateOnly tempDate = tStart; tempDate <= tEnd; tempDate = tempDate.AddDays( 1 ) )
			{
				if ( tempDate.DayOfWeek != DayOfWeek.Saturday && tempDate.DayOfWeek != DayOfWeek.Sunday
					&& ( !tempStockHolidays.TryGetValue( tempDate, out Holiday tempHoliday ) || tempHoliday.Close.HasValue )
				)
				{
					if ( tempKlinesDictionary.TryAdd( tempDate, null ) )
					{
						if ( !tempMissingStart.HasValue )
						{
							tempMissingStart = tempDate;
						}

						tempMissingEnd = tempDate;
					}
					else if ( tempMissingStart.HasValue && tempMissingEnd.HasValue )
					{
						tempMissing.Add( new( tempMissingStart.Value, tempMissingEnd.Value ) );
						tempMissingStart = null;
						tempMissingEnd = null;
					}
				}
			}

			if ( tempMissingStart.HasValue && tempMissingEnd.HasValue )
			{
				tempMissing.Add( new( tempMissingStart.Value, tempMissingEnd.Value ) );
			}

			// Get and match missing data and save to database
			List<Task> tempInsertTasks = [];
			List<Stock.SQL.DailyKline> tempBatchedKlines = [];

			await foreach ( List<Stock.SQL.DailyKline> tempKlines in GetDailyKlinesAsync( tempTicker, tempMissing, tCancel ) )
			{
				tempBatchedKlines.AddRange( tempKlines );

				while ( tempBatchedKlines.Count >= _settings.DailyKlineBatchSize )
				{
					Stock.SQL.DailyKline[] tempBatch = [ .. tempBatchedKlines.Take( _settings.DailyKlineBatchSize ) ];
					tempBatchedKlines.RemoveRange( 0, _settings.DailyKlineBatchSize );
					tempInsertTasks.Add( InsertBatchAsync( tempBatch, tCancel ) );
				}

				foreach ( Stock.SQL.DailyKline tempKline in tempKlines )
				{
					tempKlinesDictionary[ tempKline.Time ] = tempKline;
				}
			}

			foreach ( KeyValuePair<DateOnly, Stock.SQL.DailyKline> tempKVP in tempKlinesDictionary.Where( x => x.Value == null ).ToArray() )
			{
				Stock.SQL.DailyKline tempKline = new()
				{
					TickerId = tempTicker.TickerId,
					Time = tempKVP.Key,
					IsFilled = false
				};

				tempKlinesDictionary[ tempKVP.Key ] = tempKline;
				tempBatchedKlines.Add( tempKline );
			}

			while ( tempBatchedKlines.Count > 0 )
			{
				int tempRemaining = Math.Min( _settings.DailyKlineBatchSize, tempBatchedKlines.Count );
				Stock.SQL.DailyKline[] tempBatch = [ .. tempBatchedKlines.Take( tempRemaining ) ];
				tempBatchedKlines.RemoveRange( 0, tempRemaining );
				tempInsertTasks.Add( InsertBatchAsync( tempBatch, tCancel ) );
			}

			await Task.WhenAll( tempInsertTasks );

			// Output
			return tempKlinesDictionary.Where( x => x.Value?.IsFilled == true ).ToDictionary
			(
				x => x.Value.Time,
				x => new Stock.DailyKline
				(
					x.Value.Time,
					x.Value.Open,
					x.Value.High,
					x.Value.Low,
					x.Value.Close,
					x.Value.Volume
				)
			);
		}

		private async Task InsertBatchAsync( Stock.SQL.DailyKline[] tBatch, CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			DateOnly tempToday = DateOnly.FromDateTime( DateTime.UtcNow );
			List<Stock.SQL.DailyKline> tempBatch = [ .. tBatch.Where( x => x.Time < tempToday ) ];

			if ( tempBatch.Count > 0 )
			{
				using IServiceScope tempScope = _scopeFactory.CreateScope();
				Stock.SQL.Context tempContext = tempScope.ServiceProvider.GetRequiredService<Stock.SQL.Context>();
				await tempContext.DailyKline.AddRangeAsync( tempBatch, tCancel );
				await tempContext.SaveChangesAsync( tCancel );
			}
		}

		public async IAsyncEnumerable<List<Stock.SQL.DailyKline>> GetDailyKlinesAsync( Ticker tTicker, IReadOnlyCollection<Range<DateOnly>> tRequests, [EnumeratorCancellation] CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			using CancellationTokenSource tempCancelSource = CancellationTokenSource.CreateLinkedTokenSource( tCancel );
			CancellationToken tempCancel = tempCancelSource.Token;
			Task<List<Stock.SQL.DailyKline>>[] tempTasks = [ .. tRequests.Select( x => GetDailyKlinesAsync( tTicker, x, tempCancel ) ) ];

			try
			{
				await foreach ( Task<List<Stock.SQL.DailyKline>> tempTask in Task.WhenEach( tempTasks ).WithCancellation( tempCancel ) )
				{
					yield return await tempTask;
				}
			}
			finally
			{
				tempCancelSource.Cancel();

				try
				{
					await Task.WhenAll( tempTasks );
				}
				catch { }
			}
		}

		private async Task<List<Stock.SQL.DailyKline>> GetDailyKlinesAsync( Ticker tTicker, Range<DateOnly> tRange, CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			List<Stock.SQL.DailyKline> tempKlines = [];
			string tempTicker = Uri.EscapeDataString( tTicker.Name );
			string tempURL = $"{_settings.URI}/v2/aggs/ticker/{tempTicker}/range/1/day/{tRange.Min:yyyy-MM-dd}/{tRange.Max:yyyy-MM-dd}?sort=asc&limit=50000";

			try
			{
				// Get API data
				do
				{
					using HttpResponseMessage tempResponse = await GetAsync( tempURL, tCancel );
					await using Stream tempStream = await tempResponse.Content.ReadAsStreamAsync( tCancel );
					KlineResponse tempRoot = await JsonSerializer.DeserializeAsync<KlineResponse>( tempStream, cancellationToken: tCancel ) ?? throw new JsonException( $"Get DailyKlines Error: Empty response: {tRange.Min}-{tRange.Max}" );

					if ( tempRoot.Status != "OK" )
					{
						throw new InvalidDataException( $"Get DailyKlines API Error: {tempRoot.Status}: {tRange.Min}-{tRange.Max}" );
					}
					else if ( tempRoot.Results != null )
					{
						foreach ( KlineResult tempKline in tempRoot.Results )
						{
							tempKlines.Add
							(
								new()
								{
									TickerId = tTicker.TickerId,
									Time = DateOnly.FromDateTime( DateTimeOffset.FromUnixTimeMilliseconds( tempKline.Time ).UtcDateTime ),
									Open = tempKline.Open,
									High = tempKline.High,
									Low = tempKline.Low,
									Close = tempKline.Close,
									Volume = decimal.ToInt64( decimal.Truncate( tempKline.Volume ) ),
									IsFilled = true
								}
							);
						}
					}

					tempURL = string.IsNullOrWhiteSpace( tempRoot.NextURL ) ? null : tempRoot.NextURL;
				}
				while ( tempURL != null );

				return tempKlines;
			}
			catch ( OperationCanceledException ) when ( tCancel.IsCancellationRequested )
			{
				throw;
			}
			catch ( Exception tException )
			{
				_logger.LogError( tException, "Get DailyKlines Error: {Start}-{End}", tRange.Min, tRange.Max );

				throw;
			}
		}

		public async Task<Dictionary<DateOnly, Stock.TreasuryYield>> GetTreasuryYieldsAsync( HolidayFactory tHolidayFactory, DateOnly tStart, DateOnly tEnd, CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			// Get cached SQL data
			tStart = ClampStart( tStart );
			Dictionary<DateOnly, Holiday> tempBondHolidays = tHolidayFactory.GetBondMarketHolidays( tStart.Year, tEnd.Year );
			Dictionary<DateOnly, Stock.SQL.TreasuryYield> tempTreasuryYieldDictionary = [];
			List<Stock.SQL.TreasuryYield> tempSQLTreasuryYields;

			using ( IServiceScope tempScope = _scopeFactory.CreateScope() )
			{
				Stock.SQL.Context tempContext = tempScope.ServiceProvider.GetRequiredService<Stock.SQL.Context>();
				tempSQLTreasuryYields = await tempContext.TreasuryYield.AsNoTracking().Where( x => x.Time >= tStart && x.Time <= tEnd ).ToListAsync( tCancel );
			}

			foreach ( Stock.SQL.TreasuryYield tempTreasuryYield in tempSQLTreasuryYields )
			{
				tempTreasuryYieldDictionary.TryAdd( tempTreasuryYield.Time, tempTreasuryYield );
			}

			// Identify missing data
			List<Range<DateOnly>> tempMissing = [];
			DateOnly? tempMissingStart = null;
			DateOnly? tempMissingEnd = null;

			for ( DateOnly tempDate = tStart; tempDate <= tEnd; tempDate = tempDate.AddDays( 1 ) )
			{
				if ( tempDate.DayOfWeek != DayOfWeek.Saturday && tempDate.DayOfWeek != DayOfWeek.Sunday
					&& ( !tempBondHolidays.TryGetValue( tempDate, out Holiday tempHoliday ) || tempHoliday.Close.HasValue )
				)
				{
					if ( tempTreasuryYieldDictionary.TryAdd( tempDate, null ) )
					{
						if ( !tempMissingStart.HasValue )
						{
							tempMissingStart = tempDate;
						}

						tempMissingEnd = tempDate;
					}
					else if ( tempMissingStart.HasValue && tempMissingEnd.HasValue )
					{
						tempMissing.Add( new( tempMissingStart.Value, tempMissingEnd.Value ) );
						tempMissingStart = null;
						tempMissingEnd = null;
					}
				}
			}

			if ( tempMissingStart.HasValue && tempMissingEnd.HasValue )
			{
				tempMissing.Add( new( tempMissingStart.Value, tempMissingEnd.Value ) );
			}

			// Get and match missing data and save to database
			List<Task> tempInsertTasks = [];
			List<Stock.SQL.TreasuryYield> tempBatchedTreasuryYields = [];

			await foreach ( List<Stock.SQL.TreasuryYield> tempTreasuryYields in GetTreasuryYieldsAsync( tempMissing, tCancel ) )
			{
				tempBatchedTreasuryYields.AddRange( tempTreasuryYields );

				while ( tempBatchedTreasuryYields.Count >= _settings.TreasuryYieldBatchSize )
				{
					Stock.SQL.TreasuryYield[] tempBatch = [ .. tempBatchedTreasuryYields.Take( _settings.TreasuryYieldBatchSize ) ];
					tempBatchedTreasuryYields.RemoveRange( 0, _settings.TreasuryYieldBatchSize );
					tempInsertTasks.Add( InsertBatchAsync( tempBatch, tCancel ) );
				}

				foreach ( Stock.SQL.TreasuryYield tempTreasuryYield in tempTreasuryYields )
				{
					tempTreasuryYieldDictionary[ tempTreasuryYield.Time ] = tempTreasuryYield;
				}
			}

			while ( tempBatchedTreasuryYields.Count > 0 )
			{
				int tempRemaining = Math.Min( _settings.TreasuryYieldBatchSize, tempBatchedTreasuryYields.Count );
				Stock.SQL.TreasuryYield[] tempBatch = [ .. tempBatchedTreasuryYields.Take( tempRemaining ) ];
				tempBatchedTreasuryYields.RemoveRange( 0, tempRemaining );
				tempInsertTasks.Add( InsertBatchAsync( tempBatch, tCancel ) );
			}

			await Task.WhenAll( tempInsertTasks );

			// Output
			return tempTreasuryYieldDictionary.Where( x => x.Value != null ).ToDictionary
			(
				x => x.Value.Time,
				x => new Stock.TreasuryYield
				(
					x.Value.Time,
					x.Value.Yield1Month,
					x.Value.Yield3Month,
					x.Value.Yield1Year,
					x.Value.Yield5Year,
					x.Value.Yield10Year,
					x.Value.Yield30Year
				)
			);
		}

		private async Task InsertBatchAsync( Stock.SQL.TreasuryYield[] tBatch, CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			using IServiceScope tempScope = _scopeFactory.CreateScope();
			Stock.SQL.Context tempContext = tempScope.ServiceProvider.GetRequiredService<Stock.SQL.Context>();
			await tempContext.TreasuryYield.AddRangeAsync( tBatch, tCancel );
			await tempContext.SaveChangesAsync( tCancel );
		}

		public async IAsyncEnumerable<List<Stock.SQL.TreasuryYield>> GetTreasuryYieldsAsync( IReadOnlyCollection<Range<DateOnly>> tRanges, [EnumeratorCancellation] CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			using CancellationTokenSource tempCancelSource = CancellationTokenSource.CreateLinkedTokenSource( tCancel );
			CancellationToken tempCancel = tempCancelSource.Token;
			Task<List<Stock.SQL.TreasuryYield>>[] tempTasks = [ .. tRanges.Select( x => GetTreasuryYieldsAsync( x, tempCancel ) ) ];

			try
			{
				await foreach ( Task<List<Stock.SQL.TreasuryYield>> tempTask in Task.WhenEach( tempTasks ).WithCancellation( tempCancel ) )
				{
					yield return await tempTask;
				}
			}
			finally
			{
				tempCancelSource.Cancel();

				try
				{
					await Task.WhenAll( tempTasks );
				}
				catch { }
			}
		}

		private async Task<List<Stock.SQL.TreasuryYield>> GetTreasuryYieldsAsync( Range<DateOnly> tRange, CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			List<Stock.SQL.TreasuryYield> tempTreasuryYields = [];
			string tempURL = $"{_settings.URI}/fed/v1/treasury-yields?date.gte={tRange.Min:yyyy-MM-dd}&date.lte={tRange.Max:yyyy-MM-dd}&sort=date.asc&limit=50000";

			try
			{
				// Get API data
				do
				{
					using HttpResponseMessage tempResponse = await GetAsync( tempURL, tCancel );
					await using Stream tempStream = await tempResponse.Content.ReadAsStreamAsync( tCancel );
					TreasuryYieldResponse tempRoot = await JsonSerializer.DeserializeAsync<TreasuryYieldResponse>( tempStream, cancellationToken: tCancel ) ?? throw new JsonException( $"Get Treasury Yields Error: Empty response: {tRange.Min}-{tRange.Max}" );

					if ( tempRoot.Status != "OK" )
					{
						throw new InvalidDataException( $"Get Treasury Yields API Error: {tempRoot.Status}: {tRange.Min}-{tRange.Max}" );
					}
					else if ( tempRoot.Results != null )
					{
						foreach ( TreasuryYieldResult tempTreasuryYield in tempRoot.Results )
						{
							tempTreasuryYields.Add
							(
								new()
								{
									Time = tempTreasuryYield.Date,
									Yield1Month = tempTreasuryYield.Yield1Month,
									Yield3Month = tempTreasuryYield.Yield3Month,
									Yield1Year = tempTreasuryYield.Yield1Year,
									Yield5Year = tempTreasuryYield.Yield5Year,
									Yield10Year = tempTreasuryYield.Yield10Year,
									Yield30Year = tempTreasuryYield.Yield30Year
								}
							);
						}
					}

					tempURL = string.IsNullOrWhiteSpace( tempRoot.NextURL ) ? null : tempRoot.NextURL;
				}
				while ( tempURL != null );

				return tempTreasuryYields;
			}
			catch ( OperationCanceledException ) when ( tCancel.IsCancellationRequested )
			{
				throw;
			}
			catch ( Exception tException )
			{
				_logger.LogError( tException, "Get Treasury Yields Error: {Start}-{End}", tRange.Min, tRange.Max );

				throw;
			}
		}

		public async Task<Dictionary<Stock.Option, Dictionary<DateTime, Stock.Kline>>> GetOptionKlinesAsync( HolidayFactory tHolidayFactory, string tTicker, HashSet<Stock.Option> tOptions, int tIncludedDaysBeforeExpiration, int tSkipLastBeforeClose, ParallelOptions tParallelOptions )
		{
			tParallelOptions.CancellationToken.ThrowIfCancellationRequested();

			if ( tOptions.Count == 0 )
			{
				return [];
			}

			// Find Dates
			DateOnly tempEarliestStartDate = tOptions.Min( x => x.Expiration );
			DateOnly tempLatestEndDate = tOptions.Max( x => x.Expiration );
			DateTime tempNow = DateTime.UtcNow;
			DateOnly tempToday = DateOnly.FromDateTime( tempNow );
			Ticker tempTicker;
			Dictionary<Stock.Option, Stock.SQL.Option> tempOptionsDictionary = [];
			Dictionary<DateOnly, Holiday> tempStockHolidays = tHolidayFactory.GetStockMarketHolidays( tempEarliestStartDate.Year - 1, tempLatestEndDate.Year );
			tempEarliestStartDate = tempEarliestStartDate.AddDays( -tIncludedDaysBeforeExpiration );
			int tempCounter = tIncludedDaysBeforeExpiration;

			while ( tempCounter > 0 )
			{
				tempEarliestStartDate = tempEarliestStartDate.AddDays( -1 );

				if ( tempEarliestStartDate.DayOfWeek != DayOfWeek.Saturday && tempEarliestStartDate.DayOfWeek != DayOfWeek.Sunday
					&& ( !tempStockHolidays.TryGetValue( tempEarliestStartDate, out Holiday tempHoliday ) || tempHoliday.Close.HasValue ) )
				{
					--tempCounter;
				}
			}

			tempEarliestStartDate = ClampOptionKlineStart( tempEarliestStartDate );

			// Get Options and cached SQL data
			List<OptionKline> tempSQLKlines;

			using ( IServiceScope tempScope = _scopeFactory.CreateScope() )
			{
				Stock.SQL.Context tempContext = tempScope.ServiceProvider.GetRequiredService<Stock.SQL.Context>();
				tempTicker = await tempContext.Ticker.Where( x => x.Name == tTicker ).FirstOrDefaultAsync( tParallelOptions.CancellationToken );

				if ( tempTicker == null )
				{
					tempTicker = new() { Name = tTicker };
					await tempContext.Ticker.AddAsync( tempTicker, tParallelOptions.CancellationToken );
					await tempContext.SaveChangesAsync( tParallelOptions.CancellationToken );
				}

				Dictionary<(DateOnly expiration, float strike, bool isCall), Stock.SQL.Option> tempAllSQLOptions = await tempContext.Option.Where( x => x.TickerId == tempTicker.TickerId ).ToDictionaryAsync( x => (x.Expiration, x.Strike, x.IsCall), x => x, tParallelOptions.CancellationToken );

				foreach ( Stock.Option tempOption in tOptions )
				{
					if ( !tempAllSQLOptions.TryGetValue( (tempOption.Expiration, tempOption.Strike, tempOption.IsCall), out Stock.SQL.Option tempSQLOption ) )
					{
						tempSQLOption = new() { Ticker = tempTicker, Expiration = tempOption.Expiration, Strike = tempOption.Strike, IsCall = tempOption.IsCall };
						await tempContext.Option.AddAsync( tempSQLOption, tParallelOptions.CancellationToken );
					}

					tempOptionsDictionary.TryAdd( tempOption, tempSQLOption );
				}

				await tempContext.SaveChangesAsync( tParallelOptions.CancellationToken );

				HashSet<int> tempOptionIds = [ .. tempOptionsDictionary.Values.Select( x => x.OptionId ) ];
				tempSQLKlines = await tempContext.OptionKline.AsNoTracking().Where( x => tempOptionIds.Contains( x.OptionId ) && x.IncludedDaysBeforeExpiration == tIncludedDaysBeforeExpiration && x.SkipLastBeforeClose == tSkipLastBeforeClose && x.Time >= tempEarliestStartDate && x.Time <= tempLatestEndDate ).ToListAsync( tParallelOptions.CancellationToken );
			}

			ConcurrentDictionary<int, Dictionary<DateOnly, OptionKline>> tempOptionKlinesByOptionId = [];

			foreach ( OptionKline tempKline in tempSQLKlines )
			{
				if ( !tempOptionKlinesByOptionId.TryGetValue( tempKline.OptionId, out Dictionary<DateOnly, OptionKline> tempDictionary ) )
				{
					tempDictionary = [];
					tempOptionKlinesByOptionId[ tempKline.OptionId ] = tempDictionary;
				}

				tempDictionary.TryAdd( tempKline.Time, tempKline );
			}

			tempSQLKlines.Clear();

			// Identify missing data
			ConcurrentBag<OptionKlineRequest> tempMissing = [];

			Parallel.ForEach
			(
				tOptions,
				tParallelOptions,
				tOption =>
				{
					Stock.SQL.Option tempSQLOption = tempOptionsDictionary[ tOption ];

					if ( !tempOptionKlinesByOptionId.TryGetValue( tempSQLOption.OptionId, out Dictionary<DateOnly, OptionKline> tempOptionKlinesDictionary ) )
					{
						tempOptionKlinesDictionary = [];
						tempOptionKlinesByOptionId[ tempSQLOption.OptionId ] = tempOptionKlinesDictionary;
					}

					// Find start date
					DateOnly tempStartDate = tOption.Expiration;
					int tempCounter = tIncludedDaysBeforeExpiration;

					while ( tempCounter > 0 )
					{
						tempStartDate = tempStartDate.AddDays( -1 );

						if ( tempStartDate.DayOfWeek != DayOfWeek.Saturday && tempStartDate.DayOfWeek != DayOfWeek.Sunday
							&& ( !tempStockHolidays.TryGetValue( tempStartDate, out Holiday tempHoliday ) || tempHoliday.Close.HasValue ) )
						{
							--tempCounter;
						}
					}

					tempStartDate = ClampOptionKlineStart( tempStartDate );

					// Find end
					DateOnly tempOptionEndDate = tOption.Expiration < tempToday ? tOption.Expiration : tempToday;

					for ( DateOnly tempDate = tempStartDate; tempDate <= tempOptionEndDate; tempDate = tempDate.AddDays( 1 ) )
					{
						if ( tempDate.DayOfWeek != DayOfWeek.Saturday && tempDate.DayOfWeek != DayOfWeek.Sunday
							&& ( !tempStockHolidays.TryGetValue( tempDate, out Holiday tempHoliday ) || tempHoliday.Close.HasValue )
							&& tempOptionKlinesDictionary.TryAdd( tempDate, null ) )
						{
							bool tempIsDST = tHolidayFactory.GetIsDaylightSavingTime( new DateTime( tempDate, new TimeOnly( 12, 0 ), DateTimeKind.Utc ) );
							TimeOnly tempMarketOpen = tHolidayFactory.GetStockMarketOpenTime( tempIsDST );
							TimeOnly tempMarketClose = tempHoliday != null && tempHoliday.Close.HasValue ? tempHoliday.Close.Value : tHolidayFactory.GetStockMarketCloseTime( tempIsDST );
							DateTime tempOpen = new( tempDate, tempMarketOpen, DateTimeKind.Utc );
							DateTime tempClose = new( tempDate, tempMarketClose, DateTimeKind.Utc );

							if ( tempOpen <= tempNow )
							{
								//Console.WriteLine( $"( new( {tOption.Expiration.Year}, {tOption.Expiration.Month}, {tOption.Expiration.Day} ), {tOption.Strike}, {tOption.IsCall} )," );
								tempMissing.Add( new( tempSQLOption, tempOpen, tempClose, tIncludedDaysBeforeExpiration, tSkipLastBeforeClose ) );
							}
						}
					}
				}
			);

			// Get and match missing data and save to database
			List<Task> tempInsertTasks = [];
			List<OptionKline> tempBatchedKlines = [];

			await foreach ( OptionKline tempKline in GetOptionKlinesAsync( tempTicker, [ .. tempMissing ], tParallelOptions.CancellationToken ) )
			{
				tempBatchedKlines.Add( tempKline );

				while ( tempBatchedKlines.Count >= _settings.KlineBatchSize )
				{
					OptionKline[] tempBatch = [ .. tempBatchedKlines.Take( _settings.KlineBatchSize ) ];
					tempBatchedKlines.RemoveRange( 0, _settings.KlineBatchSize );
					tempInsertTasks.Add( InsertBatchAsync( tempBatch, tParallelOptions.CancellationToken ) );
				}

				if ( !tempOptionKlinesByOptionId.TryGetValue( tempKline.OptionId, out Dictionary<DateOnly, OptionKline> tempOptionKlines ) )
				{
					tempOptionKlines = [];
					tempOptionKlinesByOptionId[ tempKline.OptionId ] = tempOptionKlines;
				}

				tempOptionKlines[ tempKline.Time ] = tempKline;
			}

			while ( tempBatchedKlines.Count > 0 )
			{
				int tempRemaining = Math.Min( _settings.KlineBatchSize, tempBatchedKlines.Count );
				OptionKline[] tempBatch = [ .. tempBatchedKlines.Take( tempRemaining ) ];
				tempBatchedKlines.RemoveRange( 0, tempRemaining );
				tempInsertTasks.Add( InsertBatchAsync( tempBatch, tParallelOptions.CancellationToken ) );
			}

			await Task.WhenAll( tempInsertTasks );

			// Output
			ConcurrentBag<(Stock.Option, Dictionary<DateTime, Stock.Kline>)> tempResults = [];

			Parallel.ForEach
			(
				tOptions,
				tParallelOptions,
				tOption =>
				{
					Stock.SQL.Option tempSQLOption = tempOptionsDictionary[ tOption ];

					if ( !tempOptionKlinesByOptionId.TryGetValue( tempSQLOption.OptionId, out Dictionary<DateOnly, OptionKline> tempOptionKlinesDictionary ) )
					{
						tempOptionKlinesDictionary = [];
					}

					Dictionary<DateTime, Stock.Kline> tempNewDictionary = [];

					foreach ( KeyValuePair<DateOnly, OptionKline> tempKVP in tempOptionKlinesDictionary )
					{
						try
						{
							if ( tempKVP.Value?.Data != null )
							{
								byte[] tempData = GetDecompressed( tempKVP.Value.Data );
								using MemoryStream tempStream = new( tempData );
								using BinaryReader tempReader = new( tempStream );
								int tempKlinesLength = tempReader.ReadInt32();

								for ( int i = 0; i < tempKlinesLength; ++i )
								{
									Stock.Kline tempKline = Stock.Kline.Deserialize( tempReader );
									tempNewDictionary[ tempKline.Time ] = tempKline;
								}
							}
						}
						catch ( Exception tException )
						{
							_logger.LogError( tException, "Error deserializing Option Klines for {OptionId} {Date}", tempSQLOption.OptionId, tempKVP.Key );

							throw;
						}
					}

					tempResults.Add( (tOption, tempNewDictionary) );
				}
			);

			return tempResults.ToDictionary( x => x.Item1, x => x.Item2 );
		}

		private async Task InsertBatchAsync( OptionKline[] tBatch, CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			DateOnly tempToday = DateOnly.FromDateTime( DateTime.UtcNow );
			List<OptionKline> tempBatch = [ .. tBatch.Where( x => x.Time < tempToday ) ];

			if ( tempBatch.Count > 0 )
			{
				using IServiceScope tempScope = _scopeFactory.CreateScope();
				Stock.SQL.Context tempContext = tempScope.ServiceProvider.GetRequiredService<Stock.SQL.Context>();
				await tempContext.OptionKline.AddRangeAsync( tempBatch, tCancel );
				await tempContext.SaveChangesAsync( tCancel );
			}
		}

		public readonly record struct OptionKlineRequest( Stock.SQL.Option Option, DateTime MarketOpen, DateTime MarketClose, int IncludedDaysBeforeExpiration, int SkipLastBeforeClose );

		public async IAsyncEnumerable<OptionKline> GetOptionKlinesAsync( Ticker tTicker, IReadOnlyCollection<OptionKlineRequest> tRequests, [EnumeratorCancellation] CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			using CancellationTokenSource tempCancelSource = CancellationTokenSource.CreateLinkedTokenSource( tCancel );
			CancellationToken tempCancel = tempCancelSource.Token;
			Task<OptionKline>[] tempTasks = [ .. tRequests.Select( x => GetOptionKlineAsync( tTicker, x, tempCancel ) ) ];

			try
			{
				await foreach ( Task<OptionKline> tempTask in Task.WhenEach( tempTasks ).WithCancellation( tempCancel ) )
				{
					yield return await tempTask;
				}
			}
			finally
			{
				tempCancelSource.Cancel();

				try
				{
					await Task.WhenAll( tempTasks );
				}
				catch { }
			}
		}

		private async Task<OptionKline> GetOptionKlineAsync( Ticker tTicker, OptionKlineRequest tRequest, CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			List<Stock.Kline> tempKlines = [];
			DateOnly tempDate = DateOnly.FromDateTime( tRequest.MarketOpen );
			DateTime tempEnd = tRequest.MarketClose.AddMinutes( -( tRequest.SkipLastBeforeClose + 1 ) ); // inclusive

			try
			{
				string tempOCCString = Uri.EscapeDataString( Stock.Option.GetOCCString( tTicker.Name, tRequest.Option.Expiration, tRequest.Option.Strike, tRequest.Option.IsCall ) );
				string tempURL = $"{_settings.URI}/v2/aggs/ticker/O:{tempOCCString}/range/1/minute/{new DateTimeOffset( tRequest.MarketOpen ).ToUnixTimeMilliseconds()}/{new DateTimeOffset( tempEnd ).ToUnixTimeMilliseconds()}?adjusted=false&sort=asc&limit=50000";

				// Get API data
				do
				{
					using HttpResponseMessage tempResponse = await GetAsync( tempURL, tCancel );
					await using Stream tempContentStream = await tempResponse.Content.ReadAsStreamAsync( tCancel );
					KlineResponse tempRoot = await JsonSerializer.DeserializeAsync<KlineResponse>( tempContentStream, cancellationToken: tCancel ) ?? throw new JsonException( $"Get Option Kline Error: Empty response: {tRequest.MarketOpen}-{tempEnd}" );

					if ( tempRoot.Status != "OK" )
					{
						throw new InvalidDataException( $"Get Option Kline API Error: {tempRoot.Status}: {tRequest.MarketOpen}-{tempEnd}" );
					}
					else if ( tempRoot.Results != null )
					{
						foreach ( KlineResult tempKline in tempRoot.Results )
						{
							DateTime tempTime = DateTimeOffset.FromUnixTimeMilliseconds( tempKline.Time ).UtcDateTime;

							tempKlines.Add
							(
								new
								(
									tempTime,
									tempKline.Open,
									tempKline.High,
									tempKline.Low,
									tempKline.Close,
									decimal.ToInt64( decimal.Truncate( tempKline.Volume ) )
								)
							);
						}
					}

					tempURL = string.IsNullOrWhiteSpace( tempRoot.NextURL ) ? null : tempRoot.NextURL;
				}
				while ( tempURL != null );

				// Serialize and compress
				OptionKline tempSQLOptionKline = new()
				{
					OptionId = tRequest.Option.OptionId,
					IncludedDaysBeforeExpiration = tRequest.IncludedDaysBeforeExpiration,
					SkipLastBeforeClose = tRequest.SkipLastBeforeClose,
					Time = tempDate
				};

				using MemoryStream tempStream = new();
				using BinaryWriter tempWriter = new( tempStream );
				tempWriter.Write( tempKlines.Count );

				foreach ( Stock.Kline tempKline in tempKlines )
				{
					tempKline.Serialize( tempWriter );
				}

				tempWriter.Flush();
				tempSQLOptionKline.Data = GetCompressed( tempStream.ToArray() );

				return tempSQLOptionKline;
			}
			catch ( OperationCanceledException ) when ( tCancel.IsCancellationRequested )
			{
				throw;
			}
			catch ( Exception tException )
			{
				_logger.LogError( tException, "Get Option Kline Error: {Date}", tempDate );

				throw;
			}
		}

		public async Task<ConcurrentDictionary<DateOnly, Dictionary<DateTime, Stock.Quote>>> GetQuotesAsync( HolidayFactory tHolidayFactory, string tTicker, DateOnly tStart, DateOnly tEnd, int tSkip, int tTake, bool tIsFromStart, ParallelOptions tParallelOptions )
		{
			tParallelOptions.CancellationToken.ThrowIfCancellationRequested();

			// Get cached SQL data
			tStart = ClampStart( tStart );
			Dictionary<DateOnly, Holiday> tempStockHolidays = tHolidayFactory.GetStockMarketHolidays( tStart.Year, tEnd.Year );
			Dictionary<DateOnly, Stock.SQL.Quote> tempQuotesDictionary = [];
			Ticker tempTicker;
			List<Stock.SQL.Quote> tempSQLQuotes;

			using ( IServiceScope tempScope = _scopeFactory.CreateScope() )
			{
				Stock.SQL.Context tempContext = tempScope.ServiceProvider.GetRequiredService<Stock.SQL.Context>();
				tempTicker = await tempContext.Ticker.Where( x => x.Name == tTicker ).FirstOrDefaultAsync( tParallelOptions.CancellationToken );

				if ( tempTicker == null )
				{
					tempTicker = new() { Name = tTicker };
					await tempContext.Ticker.AddAsync( tempTicker, tParallelOptions.CancellationToken );
					await tempContext.SaveChangesAsync( tParallelOptions.CancellationToken );
				}

				tempSQLQuotes = await tempContext.Quote.AsNoTracking().Where( x => x.TickerId == tempTicker.TickerId && x.Skip == tSkip && x.Take == tTake && x.IsFromStart == tIsFromStart && x.Time >= tStart && x.Time <= tEnd ).ToListAsync( tParallelOptions.CancellationToken );
			}

			foreach ( Stock.SQL.Quote tempSQLQuote in tempSQLQuotes )
			{
				tempQuotesDictionary.TryAdd( tempSQLQuote.Time, tempSQLQuote );
			}

			tempSQLQuotes.Clear();

			// Identify missing data
			List<QuoteRequest> tempMissing = [];

			for ( DateOnly tempDate = tStart; tempDate <= tEnd; tempDate = tempDate.AddDays( 1 ) )
			{
				if ( tempDate.DayOfWeek != DayOfWeek.Saturday && tempDate.DayOfWeek != DayOfWeek.Sunday
					&& ( !tempStockHolidays.TryGetValue( tempDate, out Holiday tempHoliday ) || tempHoliday.Close.HasValue )
					&& !tempQuotesDictionary.ContainsKey( tempDate )
				)
				{
					bool tempIsDST = tHolidayFactory.GetIsDaylightSavingTime( new DateTime( tempDate, new TimeOnly( 12, 0 ), DateTimeKind.Utc ) );
					TimeOnly tempMarketOpen = tHolidayFactory.GetStockMarketOpenTime( tempIsDST );
					TimeOnly tempMarketClose = tempHoliday != null && tempHoliday.Close.HasValue ? tempHoliday.Close.Value : tHolidayFactory.GetStockMarketCloseTime( tempIsDST );

					tempMissing.Add
					(
						new
						(
							new DateTime( tempDate, tempMarketOpen, DateTimeKind.Utc ),
							new DateTime( tempDate, tempMarketClose, DateTimeKind.Utc ),
							tSkip,
							tTake,
							tIsFromStart
						)
					);
				}
			}

			// Get and match missing data and save to database
			List<Task> tempInsertTasks = [];
			List<Stock.SQL.Quote> tempBatchedQuotes = [];

			await foreach ( Stock.SQL.Quote tempQuote in GetQuotesAsync( tempTicker, tempMissing, tParallelOptions.CancellationToken ) )
			{
				tempBatchedQuotes.Add( tempQuote );

				while ( tempBatchedQuotes.Count >= _settings.QuoteBatchSize )
				{
					Stock.SQL.Quote[] tempBatch = [ .. tempBatchedQuotes.Take( _settings.QuoteBatchSize ) ];
					tempBatchedQuotes.RemoveRange( 0, _settings.QuoteBatchSize );
					tempInsertTasks.Add( InsertBatchAsync( tempBatch, tParallelOptions.CancellationToken ) );
				}

				tempQuotesDictionary[ tempQuote.Time ] = tempQuote;
			}

			while ( tempBatchedQuotes.Count > 0 )
			{
				int tempRemaining = Math.Min( _settings.QuoteBatchSize, tempBatchedQuotes.Count );
				Stock.SQL.Quote[] tempBatch = [ .. tempBatchedQuotes.Take( tempRemaining ) ];
				tempBatchedQuotes.RemoveRange( 0, tempRemaining );
				tempInsertTasks.Add( InsertBatchAsync( tempBatch, tParallelOptions.CancellationToken ) );
			}

			await Task.WhenAll( tempInsertTasks );

			// Output
			ConcurrentDictionary<DateOnly, Dictionary<DateTime, Stock.Quote>> tempDeserialized = [];

			Parallel.ForEach
			(
				tempQuotesDictionary,
				tParallelOptions,
				x =>
				{
					try
					{
						if ( x.Value?.Data != null )
						{
							byte[] tempData = GetDecompressed( x.Value.Data );
							using MemoryStream tempStream = new( tempData, writable: false );
							using BinaryReader tempReader = new( tempStream );
							int tempQuotesLength = tempReader.ReadInt32();
							Dictionary<DateTime, Stock.Quote> tempQuotes = new( tempQuotesLength );

							for ( int i = 0; i < tempQuotesLength; ++i )
							{
								Stock.Quote tempQuote = Stock.Quote.Deserialize( tempReader );
								tempQuotes[ tempQuote.Time ] = tempQuote;
							}

							tempDeserialized.TryAdd( x.Key, tempQuotes );
						}
					}
					catch ( Exception tException )
					{
						_logger.LogError( tException, "Error deserializing Quotes for {Date}", x.Key );

						throw;
					}
				}
			);

			return tempDeserialized;
		}

		private async Task InsertBatchAsync( Stock.SQL.Quote[] tBatch, CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			DateOnly tempToday = DateOnly.FromDateTime( DateTime.UtcNow );
			List<Stock.SQL.Quote> tempBatch = [ .. tBatch.Where( x => x.Time < tempToday ) ];

			if ( tempBatch.Count > 0 )
			{
				using IServiceScope tempScope = _scopeFactory.CreateScope();
				Stock.SQL.Context tempContext = tempScope.ServiceProvider.GetRequiredService<Stock.SQL.Context>();
				await tempContext.Quote.AddRangeAsync( tempBatch, tCancel );
				await tempContext.SaveChangesAsync( tCancel );
			}
		}

		public readonly record struct QuoteRequest( DateTime MarketOpen, DateTime MarketClose, int Skip, int Take, bool IsFromStart );

		public async IAsyncEnumerable<Stock.SQL.Quote> GetQuotesAsync( Ticker tTicker, IReadOnlyCollection<QuoteRequest> tRequests, [EnumeratorCancellation] CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			using CancellationTokenSource tempCancelSource = CancellationTokenSource.CreateLinkedTokenSource( tCancel );
			CancellationToken tempCancel = tempCancelSource.Token;
			Task<Stock.SQL.Quote>[] tempTasks = [ .. tRequests.Select( x => GetQuoteAsync( tTicker, x, tempCancel ) ) ];

			try
			{
				await foreach ( Task<Stock.SQL.Quote> tempTask in Task.WhenEach( tempTasks ).WithCancellation( tempCancel ) )
				{
					yield return await tempTask;
				}
			}
			finally
			{
				tempCancelSource.Cancel();

				try
				{
					await Task.WhenAll( tempTasks );
				}
				catch { }
			}
		}

		private async Task<Stock.SQL.Quote> GetQuoteAsync( Ticker tTicker, QuoteRequest tRequest, CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			List<Stock.Quote> tempQuotes = [];
			DateOnly tempDate = DateOnly.FromDateTime( tRequest.MarketClose );
			DateTime tempEnd;

			if ( tRequest.IsFromStart )
			{
				tempEnd = tRequest.MarketOpen.AddMinutes( tRequest.Skip + 1 );
			}
			else
			{
				tempEnd = tRequest.MarketClose.AddMinutes( -tRequest.Skip );
			}

			DateTime tempStart = tempEnd.AddMinutes( -tRequest.Take );

			try
			{
				long tempStartNanoseconds = new DateTimeOffset( tempStart ).ToUnixTimeMilliseconds() * 1_000_000L;
				long tempEndNanoseconds = new DateTimeOffset( tempEnd ).ToUnixTimeMilliseconds() * 1_000_000L;
				string tempTicker = Uri.EscapeDataString( tTicker.Name );
				string tempURL = $"{_settings.URI}/v3/quotes/{tempTicker}?timestamp.gte={tempStartNanoseconds}&timestamp.lt={tempEndNanoseconds}&order=asc&sort=timestamp&limit=50000";

				// Get API data
				do
				{
					using HttpResponseMessage tempResponse = await GetAsync( tempURL, tCancel );
					await using Stream tempContentStream = await tempResponse.Content.ReadAsStreamAsync( tCancel );
					QuoteResponse tempRoot = await JsonSerializer.DeserializeAsync<QuoteResponse>( tempContentStream, cancellationToken: tCancel ) ?? throw new JsonException( $"Get Quotes Error: Empty response: {tempStart}-{tempEnd}" );

					if ( tempRoot.Status != "OK" )
					{
						throw new InvalidDataException( $"Get Quotes API Error: {tempRoot.Status}: {tempStart}-{tempEnd}" );
					}
					else if ( tempRoot.Results != null )
					{
						foreach ( QuoteResult tempQuote in tempRoot.Results )
						{
							DateTime tempTime = tempQuote.ParticipantTimestamp.ToUtcDateTimeFromNanoseconds();

							tempQuotes.Add
							(
								new
								(
									tempTime,
									tempQuote.AskPrice,
									tempQuote.BidPrice,
									(long)tempQuote.AskSize,
									(long)tempQuote.BidSize
								)
							);
						}
					}

					tempURL = string.IsNullOrWhiteSpace( tempRoot.NextURL ) ? null : tempRoot.NextURL;
				}
				while ( tempURL != null );

				// Serialize and compress
				Stock.SQL.Quote tempSQLQuote = new()
				{
					TickerId = tTicker.TickerId,
					Skip = tRequest.Skip,
					Take = tRequest.Take,
					IsFromStart = tRequest.IsFromStart,
					Time = tempDate
				};

				using MemoryStream tempStream = new();
				using BinaryWriter tempWriter = new( tempStream );
				tempWriter.Write( tempQuotes.Count );

				foreach ( Stock.Quote tempQuote in tempQuotes )
				{
					tempQuote.Serialize( tempWriter );
				}

				tempWriter.Flush();
				tempSQLQuote.Data = GetCompressed( tempStream.ToArray() );

				return tempSQLQuote;
			}
			catch ( OperationCanceledException ) when ( tCancel.IsCancellationRequested )
			{
				throw;
			}
			catch ( Exception tException )
			{
				_logger.LogError( tException, "Get Quotes Error: {Date}", tempDate );

				throw;
			}
		}

		public async Task<ConcurrentDictionary<DateTime, Crypto.Kline>> GetCryptoKlinesAsync( string tSymbol, DateTime tStart, DateTime tEnd, ParallelOptions tParallelOptions )
		{
			tParallelOptions.CancellationToken.ThrowIfCancellationRequested();
			
			// Get cached SQL data
			Crypto.SQL.Symbol tempSymbol;
			Crypto.SQL.Kline tempSQLKline;

			using ( IServiceScope tempScope = _scopeFactory.CreateScope() )
			{
				Crypto.SQL.Context tempContext = tempScope.ServiceProvider.GetRequiredService<Crypto.SQL.Context>();
				tempSymbol = await tempContext.Symbol.Where( x => x.Name == tSymbol ).FirstOrDefaultAsync( tParallelOptions.CancellationToken );

				if ( tempSymbol == null )
				{
					tempSymbol = new() { Name = tSymbol };
					await tempContext.Symbol.AddAsync( tempSymbol, tParallelOptions.CancellationToken );
					await tempContext.SaveChangesAsync( tParallelOptions.CancellationToken );
				}

				tempSQLKline = await tempContext.Kline.AsNoTracking().Where( x => x.SymbolId == tempSymbol.SymbolId && x.StartTime == tStart && x.EndTime == tEnd ).FirstOrDefaultAsync( tParallelOptions.CancellationToken );
			}

			// Output cached data
			if ( tempSQLKline?.Data != null )
			{
				try
				{
					ConcurrentDictionary<DateTime, Crypto.Kline> tempDeserialized = [];
					byte[] tempData = GetDecompressed( tempSQLKline.Data );
					using MemoryStream tempStream = new( tempData );
					using BinaryReader tempReader = new( tempStream );
					int tempKlinesLength = tempReader.ReadInt32();

					for ( int i = 0; i < tempKlinesLength; ++i )
					{
						Crypto.Kline tempKline = Crypto.Kline.Deserialize( tempReader );

						if ( tempKline.Time >= tStart && tempKline.Time < tEnd )
						{
							tempDeserialized.TryAdd( tempKline.Time, tempKline );
						}
					}

					return tempDeserialized;
				}
				catch ( Exception tException )
				{
					_logger.LogError( tException, "Error deserializing Crypto Klines for {Symbol}: {Start}-{End}", tSymbol, tStart, tEnd );

					throw;
				}
			}

			// Split API requests at the 50,000 base aggregate limit
			List<CryptoKlineRequest> tempRequests = [];
			DateTime tempRequestStart = tStart;

			while ( tempRequestStart < tEnd )
			{
				DateTime tempRequestEnd = tempRequestStart.AddMinutes( 50000 );

				if ( tempRequestEnd > tEnd )
				{
					tempRequestEnd = tEnd;
				}

				tempRequests.Add( new( tempRequestStart, tempRequestEnd ) );
				tempRequestStart = tempRequestEnd;
			}

			// Get API data
			ConcurrentDictionary<DateTime, Crypto.Kline> tempKlines = [];

			await Parallel.ForEachAsync
			(
				tempRequests,
				tParallelOptions,
				async ( tempRequest, tCancel ) =>
				{
					List<Crypto.Kline> tempRequestKlines = await GetCryptoKlinesAsync( tSymbol, tempRequest, tCancel );

					foreach ( Crypto.Kline tempKline in tempRequestKlines )
					{
						if ( tempKline.Time >= tStart && tempKline.Time < tEnd )
						{
							tempKlines[ tempKline.Time ] = tempKline;
						}
					}
				}
			);

			// Serialize and compress
			DateTime tempCurrentMinute = DateTime.UtcNow;
			tempCurrentMinute = tempCurrentMinute.AddTicks( -( tempCurrentMinute.Ticks % TimeSpan.TicksPerMinute ) );

			if ( tEnd <= tempCurrentMinute )
			{
				Crypto.SQL.Kline tempNewSQLKline = new()
				{
					SymbolId = tempSymbol.SymbolId,
					StartTime = tStart,
					EndTime = tEnd
				};

				using MemoryStream tempStream = new();
				using BinaryWriter tempWriter = new( tempStream );
				Crypto.Kline[] tempOrderedKlines = [ .. tempKlines.Values.OrderBy( x => x.Time ) ];

				tempWriter.Write( tempOrderedKlines.Length );

				foreach ( Crypto.Kline tempKline in tempOrderedKlines )
				{
					tempKline.Serialize( tempWriter );
				}

				tempWriter.Flush();
				tempNewSQLKline.Data = GetCompressed( tempStream.ToArray() );

				using IServiceScope tempScope = _scopeFactory.CreateScope();
				Crypto.SQL.Context tempContext = tempScope.ServiceProvider.GetRequiredService<Crypto.SQL.Context>();
				await tempContext.Kline.AddAsync( tempNewSQLKline, tParallelOptions.CancellationToken );
				await tempContext.SaveChangesAsync( tParallelOptions.CancellationToken );
			}

			return tempKlines;
		}

		private readonly record struct CryptoKlineRequest( DateTime Start, DateTime End ); // End is exclusive

		private async Task<List<Crypto.Kline>> GetCryptoKlinesAsync( string tSymbol, CryptoKlineRequest tRequest, CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			List<Crypto.Kline> tempKlines = [];
			long tempStartMilliseconds = new DateTimeOffset( tRequest.Start ).ToUnixTimeMilliseconds();
			long tempEndMilliseconds = new DateTimeOffset( tRequest.End ).ToUnixTimeMilliseconds() - 1; // API end is inclusive
			string tempTicker = Uri.EscapeDataString( $"X:{tSymbol}" );
			string tempURL = $"{_settings.URI}/v2/aggs/ticker/{tempTicker}/range/1/minute/{tempStartMilliseconds}/{tempEndMilliseconds}?sort=asc&limit=50000";

			try
			{
				// Get API data
				do
				{
					using HttpResponseMessage tempResponse = await GetAsync( tempURL, tCancel );
					await using Stream tempStream = await tempResponse.Content.ReadAsStreamAsync( tCancel );
					CryptoKlineResponse tempRoot = await JsonSerializer.DeserializeAsync<CryptoKlineResponse>( tempStream, cancellationToken: tCancel ) ?? throw new JsonException( $"Get Crypto Klines Error: Empty response: {tRequest.Start}-{tRequest.End}" );

					if ( tempRoot.Status != "OK" )
					{
						throw new InvalidDataException( $"Get Crypto Klines API Error: {tempRoot.Status}: {tRequest.Start}-{tRequest.End}" );
					}
					else if ( tempRoot.Results != null )
					{
						foreach ( CryptoKlineResult tempKline in tempRoot.Results )
						{
							DateTime tempTime = DateTimeOffset.FromUnixTimeMilliseconds( tempKline.Time ).UtcDateTime;

							if ( tempTime >= tRequest.Start && tempTime < tRequest.End )
							{
								tempKlines.Add
								(
									new
									(
										tempTime,
										tempKline.Open,
										tempKline.High,
										tempKline.Low,
										tempKline.Close,
										tempKline.Volume,
										tempKline.WeightedVolumePrice,
										tempKline.TradeCount
									)
								);
							}
						}
					}

					tempURL = string.IsNullOrWhiteSpace( tempRoot.NextURL ) ? null : tempRoot.NextURL;
				}
				while ( tempURL != null );

				return tempKlines;
			}
			catch ( OperationCanceledException ) when ( tCancel.IsCancellationRequested )
			{
				throw;
			}
			catch ( Exception tException )
			{
				_logger.LogError( tException, "Get Crypto Klines Error: {Symbol}: {Start}-{End}", tSymbol, tRequest.Start, tRequest.End );

				throw;
			}
		}

		private async Task<HttpResponseMessage> GetAsync( string tURI, CancellationToken tCancel )
		{
			tCancel.ThrowIfCancellationRequested();

			HttpClient tempClient = _httpClientFactory.CreateClient( NAME );
			string tempSeparator = tURI.Contains( '?' ) ? "&" : "?";
			string tempRequestURI = $"{tURI}{tempSeparator}apiKey={Uri.EscapeDataString( _settings.APIKey )}";

			for ( int tempAttempt = 1; tempAttempt <= _settings.RetryAmount; ++tempAttempt )
			{
				// Wait for rate limit
				using RateLimitLease tempLease = await _HTTPRateLimiter.AcquireAsync( 1, tCancel );

				if ( !tempLease.IsAcquired )
				{
					throw new HttpRequestException( "Polygon HTTP rate limiter rejected the request." );
				}

				// Request
				HttpResponseMessage tempResponse;

				try
				{
					_logger.LogDebug( "Get Attempt {Attempt}/{Attempts}: {URI}", tempAttempt, _settings.RetryAmount, tURI );

					tempResponse = await tempClient.GetAsync( tempRequestURI, HttpCompletionOption.ResponseHeadersRead, tCancel );
				}
				catch ( OperationCanceledException ) when ( tCancel.IsCancellationRequested )
				{
					throw;
				}
				catch ( Exception tException ) when ( tException is HttpRequestException || tException is OperationCanceledException )
				{
					if ( tempAttempt >= _settings.RetryAmount )
					{
						throw new HttpRequestException( "Failed to send HTTP request after retries.", tException );
					}

					TimeSpan tempRetryDelay = GetRetryDelay( tempAttempt );

					_logger.LogWarning( tException, "HTTP request failed. Retrying attempt {NextAttempt}/{Attempts} in {Delay}ms", tempAttempt + 1, _settings.RetryAmount, (int)tempRetryDelay.TotalMilliseconds );

					await Task.Delay( tempRetryDelay, tCancel );
					continue;
				}
				catch ( Exception tException )
				{
					throw new HttpRequestException( "Failed to send HTTP request.", tException );
				}

				// Process, or check if we should retry the request
				if ( tempResponse.IsSuccessStatusCode )
				{
					return tempResponse;
				}

				HttpStatusCode tempStatusCode = tempResponse.StatusCode;
				string tempReasonPhrase = tempResponse.ReasonPhrase;
				bool tempIsRateLimited = tempStatusCode == HttpStatusCode.TooManyRequests || (int)tempStatusCode == 418;
				bool tempIsRetry = tempIsRateLimited || tempStatusCode == HttpStatusCode.RequestTimeout || (int)tempStatusCode >= 500;

				if ( !tempIsRetry || tempAttempt >= _settings.RetryAmount )
				{
					tempResponse.Dispose();

					throw new HttpRequestException( tempReasonPhrase ?? $"HTTP {(int)tempStatusCode}", null, tempStatusCode );
				}

				TimeSpan tempDelay = tempResponse.Headers.RetryAfter != null ? GetRetryAfterDelay( tempResponse ) : tempIsRateLimited ? TimeSpan.FromSeconds( 60 ) : GetRetryDelay( tempAttempt );

				_logger.LogWarning( "HTTP {StatusCode}. Retrying attempt {NextAttempt}/{Attempts} in {Delay}ms", (int)tempStatusCode, tempAttempt + 1, _settings.RetryAmount, (int)tempDelay.TotalMilliseconds );

				tempResponse.Dispose();
				await Task.Delay( tempDelay, tCancel );
			}

			throw new UnreachableException();
		}

		private static TimeSpan GetRetryDelay( int tAttempt )
		{
			int tempDelay = Math.Min( 30000, 1000 * ( 1 << Math.Min( tAttempt - 1, 5 ) ) ) + Random.Shared.Next( 0, 1000 );

			return TimeSpan.FromMilliseconds( tempDelay );
		}

		private static TimeSpan GetRetryAfterDelay( HttpResponseMessage tResponse )
		{
			if ( tResponse.Headers.RetryAfter?.Delta is TimeSpan tempDelta )
			{
				return tempDelta > TimeSpan.Zero ? tempDelta : TimeSpan.Zero;
			}
			else if ( tResponse.Headers.RetryAfter?.Date is DateTimeOffset tempDate )
			{
				TimeSpan tempDelay = tempDate - DateTimeOffset.UtcNow;

				return tempDelay > TimeSpan.Zero ? tempDelay : TimeSpan.Zero;
			}

			return TimeSpan.FromSeconds( 60 );
		}

		private static byte[] GetCompressed( byte[] tData )
		{
			if ( tData != null && tData.Length > 0 )
			{
				int tempMaxLength = LZ4Codec.MaximumOutputSize( tData.Length );
				byte[] tempCompressed = new byte[ sizeof( int ) + tempMaxLength ];

				BitConverter.TryWriteBytes( tempCompressed.AsSpan( 0, sizeof( int ) ), tData.Length );

				int tempCompressedLength = LZ4Codec.Encode
				(
					tData,
					0,
					tData.Length,
					tempCompressed,
					sizeof( int ),
					tempMaxLength,
					LZ4Level.L00_FAST
				);

				Array.Resize( ref tempCompressed, sizeof( int ) + tempCompressedLength );

				return tempCompressed;
			}

			return tData;
		}

		private static byte[] GetDecompressed( byte[] tData )
		{
			if ( tData != null && tData.Length > 0 )
			{
				int tempUncompressedLength = BitConverter.ToInt32( tData, 0 );
				byte[] tempDecompressed = new byte[ tempUncompressedLength ];
				int tempDecodedLength = LZ4Codec.Decode
				(
					tData,
					sizeof( int ),
					tData.Length - sizeof( int ),
					tempDecompressed,
					0,
					tempDecompressed.Length
				);

				if ( tempDecodedLength != tempUncompressedLength )
				{
					throw new InvalidDataException( $"Invalid Quote data length. Expected {tempUncompressedLength}, got {tempDecodedLength}." );
				}

				return tempDecompressed;
			}

			return tData;
		}
	}
}
