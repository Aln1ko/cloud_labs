from prometheus_client import Counter, Histogram,Gauge

prom_counter = Counter(
    "http_requests_total",
    "Total number of requsts",
    labelnames=["service", "method", "endpoint", "status_code"])

prom_histogram = Histogram(
    "http_request_duration_second",
    "samples_observation_and_count_in_buckets_for_request(latency)",
    labelnames=["service", "method", "endpoint"])

cluster_alive_replicas = Gauge(
    "cluster_alive_replicas",
    "number_woking_replicas_for_each_shard",
    labelnames=["shard_id"])

cluster_dead_replicas = Gauge(
    "cluster_dead_replicas",
    "number_not_woking_replicas_for_each_shard",
    labelnames=["shard_id"])

shard_replication_lag = Gauge(
    "shard_replication_lag",
    "difference_between_leader_and_replicas",
    labelnames=["shard_id","replica_url"]
)