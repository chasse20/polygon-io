using System.Text.Json.Serialization;

namespace PolygonMarketData.API
{
	public sealed class KlineResult
	{
		[JsonPropertyName( "t" )]
		public long Time { get; set; }
		[JsonPropertyName( "o" )]
		public decimal Open { get; set; }
		[JsonPropertyName( "h" )]
		public decimal High { get; set; }
		[JsonPropertyName( "l" )]
		public decimal Low { get; set; }
		[JsonPropertyName( "c" )]
		public decimal Close { get; set; }
		[JsonPropertyName( "v" )]
		public decimal Volume { get; set; }
	}
}
