import pandas as pd
import redis
import time
import random
import os

r = redis.Redis(host="redis", port=6379, decode_responses=True)
stream = "cc_stream"

DATA_PATH = os.getenv("DATA_PATH", "./data/creditcard.csv")
df = pd.read_csv(DATA_PATH)

MIN_DELAY = 3
MAX_DELAY = 5

MAX_ROWS = 1000  # or 100, 500, etc.
#"Time","V1","V2","V3","V4","V5","V6","V7","V8","V9","V10","V11","V12","V13","V14","V15","V16","V17","V18","V19","V20","V21","V22","V23","V24","V25","V26","V27","V28","Amount","Class"

for idx, row in df.head(MAX_ROWS).iterrows():
    event = {
        "Time": str(row["Time"]),
        "V1": str(row["V1"]),
        "V2": str(row["V2"]),
        "V3": str(row["V3"]),
        "V4": str(row["V4"]),
        "V5": str(row["V5"]),
        "V6": str(row["V6"]),
        "V7": str(row["V7"]),
        "V8": str(row["V8"]),
        "V9": str(row["V9"]),
        "V10": str(row["V10"]),
        "V11": str(row["V11"]),
        "V12": str(row["V12"]),
        "V13": str(row["V13"]),
        "V14": str(row["V14"]),
        "V15": str(row["V15"]),
        "V16": str(row["V16"]),
        "V17": str(row["V17"]),
        "V18": str(row["V18"]),
        "V19": str(row["V19"]),
        "V20": str(row["V20"]),
        "V21": str(row["V21"]),
        "V22": str(row["V22"]),
        "V23": str(row["V23"]),
        "V24": str(row["V24"]),
        "V25": str(row["V25"]),
        "V26": str(row["V26"]),
        "V27": str(row["V27"]),
        "V28": str(row["V28"]),
        "Amount": str(row["Amount"]),
        "Class": str(row["Class"])
    }

    msg_id = r.xadd(stream, event)
    delay = random.uniform(MIN_DELAY, MAX_DELAY)

    print(f"PUSH {msg_id} | Class={row['Class']} | next in {delay:.2f}s")

    time.sleep(delay)
