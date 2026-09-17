# Polygon Market Data Service

This repository is a sanitized code sample extracted from a larger personal financial project I have been developing since 2022 as a hobby.

The service uses Polygon.io (now Massive) as a market data source and was built to retrieve, cache, and process large historical datasets efficiently with throughputs capable of handling millions of records in only a few seconds.

- Concurrent and Parallelized request pipelines using TPL
- Tokenized API rate limiting independent of request concurrency
- Retry/backoff handling for rate limits, timeouts, and server errors
- Streaming JSON deserialization from HTTP responses
- SQL backed caching that requests only missing date ranges
- Batched asynchronous persistence
- LZ4 compression for cached intraday datasets
- Market calendar, holiday, partial days, and DST-aware retrieval
- Equity, option, quote, cryptocurrency, and Treasury-yield data paths

## Architecture

The service first checks the local database for requested data, identifies gaps, retrieves only the missing ranges, persists completed batches, and returns the combined result. Independent requests can run concurrently while a shared rate limiter controls outbound API throughput.

For larger request sets, results are consumed as individual operations finish instead of waiting for them in submission order. Cached binary time series data is decompressed and deserialized in parallel where appropriate.
