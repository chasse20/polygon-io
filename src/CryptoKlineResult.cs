using System.Text.Json.Serialization;

namespace PolygonMarketData.API
{
	public sealed class CryptoKlineResult
	{
		[JsonRequired]
		[JsonPropertyName( "t" )]
		public long Time { get; set; }
		[JsonRequired]
		[JsonPropertyName( "o" )]
		public decimal Open { get; set; }
		[JsonRequired]
		[JsonPropertyName( "h" )]
		public decimal High { get; set; }
		[JsonRequired]
		[JsonPropertyName( "l" )]
		public decimal Low { get; set; }
		[JsonRequired]
		[JsonPropertyName( "c" )]
		public decimal Close { get; set; }
		[JsonRequired]
		[JsonPropertyName( "v" )]
		public decimal Volume { get; set; }
		[JsonRequired]
		[JsonPropertyName( "vw" )]
		public decimal WeightedVolumePrice { get; set; }
		[JsonRequired]
		[JsonPropertyName( "n" )]
		public long TradeCount { get; set; }
	}
}
