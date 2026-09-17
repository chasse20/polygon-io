using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace PolygonMarketData.API
{
	public sealed class QuoteResponse
	{
		[JsonRequired]
		[JsonPropertyName( "status" )]
		public string Status { get; set; }
		[JsonPropertyName( "results" )]
		public List<QuoteResult> Results { get; set; }
		[JsonPropertyName( "next_url" )]
		public string NextURL { get; set; }
	}
}
