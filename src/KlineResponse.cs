using System.Text.Json.Serialization;

namespace PolygonMarketData.API
{
	public sealed class KlineResponse
	{
		[JsonPropertyName( "status" )]
		public string Status { get; set; }
		[JsonPropertyName( "results" )]
		public KlineResult[] Results { get; set; }
		[JsonPropertyName( "next_url" )]
		public string NextURL { get; set; }
	}
}
