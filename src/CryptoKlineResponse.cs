using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace PolygonMarketData.API
{
	public sealed class CryptoKlineResponse
	{
		[JsonRequired]
		[JsonPropertyName( "status" )]
		public string Status { get; set; }
		[JsonPropertyName( "results" )]
		public List<CryptoKlineResult> Results { get; set; }
		[JsonPropertyName( "next_url" )]
		public string NextURL { get; set; }
	}
}
