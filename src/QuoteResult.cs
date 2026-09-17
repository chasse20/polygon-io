using System.Text.Json.Serialization;

namespace PolygonMarketData.API
{
	public sealed class QuoteResult
	{
		[JsonRequired]
		[JsonPropertyName( "participant_timestamp" )]
		public long ParticipantTimestamp { get; set; }
		[JsonRequired]
		[JsonPropertyName( "ask_price" )]
		public decimal AskPrice { get; set; }
		[JsonRequired]
		[JsonPropertyName( "bid_price" )]
		public decimal BidPrice { get; set; }
		[JsonRequired]
		[JsonPropertyName( "ask_size" )]
		public decimal AskSize { get; set; }
		[JsonRequired]
		[JsonPropertyName( "bid_size" )]
		public decimal BidSize { get; set; }
	}
}
