from prometheus_client import Counter

recommendations_served = Counter(
    "recommendations_served_total",
    "Total recommendations served",
    ["source"],
)
