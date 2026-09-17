namespace PolygonMarketData.API
{
	public class Settings
	{
		public int RetryAmount { get; set; }
		public int KlineBatchSize { get; set; }
		public int DailyKlineBatchSize { get; set; }
		public int TreasuryYieldBatchSize { get; set; }
		public int QuoteBatchSize { get; set; }
		public string URI { get; set; }
		public string APIKey { get; set; }
	}
}
