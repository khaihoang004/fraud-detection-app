CREATE KEYSPACE IF NOT EXISTS fraud_detection
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS fraud_detection.predictions_by_day_asc (
  day text,
  event_ts timestamp,
  event_id text,
  time double,
  amount double,
  ground_truth int,
  prediction_score double,
  class text,
  PRIMARY KEY ((day), event_ts, event_id)
) WITH CLUSTERING ORDER BY (event_ts ASC);
