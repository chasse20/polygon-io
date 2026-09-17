using System;
using System.Text.Json.Serialization;

namespace PolygonMarketData.API
{
	public sealed class TreasuryYieldResult
	{
		[JsonRequired]
		[JsonPropertyName( "date" )]
		public DateOnly Date { get; set; }
		[JsonPropertyName( "yield_1_month" )]
		public decimal? Yield1Month { get; set; }
		[JsonPropertyName( "yield_3_month" )]
		public decimal? Yield3Month { get; set; }
		[JsonPropertyName( "yield_1_year" )]
		public decimal? Yield1Year { get; set; }
		[JsonPropertyName( "yield_5_year" )]
		public decimal? Yield5Year { get; set; }
		[JsonPropertyName( "yield_10_year" )]
		public decimal? Yield10Year { get; set; }
		[JsonPropertyName( "yield_30_year" )]
		public decimal? Yield30Year { get; set; }
	}
}
