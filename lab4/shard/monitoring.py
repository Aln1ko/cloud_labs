from prometheus_client import Counter, Histogram,Gauge

prom_counter = Counter(
    "http_requests_total",
    "Total number of requsts",
    labelnames=["service", "method", "endpoint", "status_code"])

prom_histogram = Histogram(
    "http_request_duration_second",
    "samples_observation_and_count_in_buckets_for_request(latency)",
    labelnames=["service", "method", "endpoint"])

shard_records_count = Gauge(
    "shard_records_count",
    "number_record_for_each_shard",
    labelnames=["shard_id", "replica_id", "role", "table_name"])

