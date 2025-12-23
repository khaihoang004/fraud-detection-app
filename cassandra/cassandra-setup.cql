CREATE KEYSPACE IF NOT EXISTS fraud_detection 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE fraud_detection;

CREATE TABLE IF NOT EXISTS fraud_events (
    day text,
    timestamp timestamp,
    transaction_id uuid,
    amount double,
    time_delta double,
    v1 double, v2 double, v3 double, v4 double, v5 double,
    v6 double, v7 double, v8 double, v9 double, v10 double,
    v11 double, v12 double, v13 double, v14 double, v15 double,
    v16 double, v17 double, v18 double, v19 double, v20 double,    
    v21 double, v22 double, v23 double, v24 double, v25 double,
    v26 double, v27 double, v28 double
    
    fraud_score double,
    is_fraud boolean,
    triggered_rules list<text>,
    PRIMARY KEY ((day), timestamp, transaction_id)
) WITH CLUSTERING ORDER BY (timestamp DESC);