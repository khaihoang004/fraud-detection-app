CREATE KEYSPACE IF NOT EXISTS fraud_detection
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS fraud_detection.predictions_by_day (
  day text,
  event_ts timestamp,
  event_id text,
  time double,
  amount double,
  class int,
  prediction_score double,
  PRIMARY KEY ((day), event_ts, event_id)
) WITH CLUSTERING ORDER BY (event_ts DESC);
