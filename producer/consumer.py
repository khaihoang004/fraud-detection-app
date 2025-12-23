import os
import time
import redis
import joblib
import pandas as pd

STREAM_IN = os.getenv("STREAM_IN", "cc_stream")
GROUP = os.getenv("GROUP", "cc_group")
CONSUMER = os.getenv("CONSUMER", "infer-1")

STREAM_OUT = os.getenv("STREAM_OUT", "cc_scored_stream")
WRITE_SCORED = os.getenv("WRITE_SCORED", "1") == "1"

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
BLOCK_MS = int(os.getenv("BLOCK_MS", "5000"))

MODEL_PATH = os.getenv("MODEL_PATH", "LogisticRegression.pkl")

# ✅ Use only features your model was trained on
FEATURES = ["V17", "V14", "V12", "V10", "V16", "V3", "V7"]

# Optional: keep some passthrough fields for dashboard display (if present)
PASSTHROUGH = ["Time", "Amount"]  # safe to keep; will be included if producer sends them

r = redis.Redis(host="redis", port=6379, decode_responses=True)


def wait_for_redis():
    while True:
        try:
            r.ping()
            return
        except redis.exceptions.ConnectionError:
            print("Waiting for Redis...", flush=True)
            time.sleep(0.5)


def ensure_group():
    try:
        r.xgroup_create(STREAM_IN, GROUP, id="0", mkstream=True)
        print(f"✅ group ready: {GROUP} on {STREAM_IN}", flush=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            return
        raise


def parse_batch(messages):
    """
    Convert Redis messages -> DataFrame + ids + passthrough fields.
    Skips records missing required FEATURES or with non-float values.
    """
    rows = []
    ids = []
    passthrough_rows = []
    skipped = 0

    for msg_id, fields in messages:
        try:
            row = {f: float(fields[f]) for f in FEATURES}
            rows.append(row)
            ids.append(msg_id)

            extra = {}
            for k in PASSTHROUGH:
                if k in fields:
                    extra[k] = fields[k]
            passthrough_rows.append(extra)

        except Exception:
            skipped += 1

    if skipped:
        print(f"⚠️ skipped {skipped} records (missing/bad required features)", flush=True)

    if not rows:
        return None, [], []

    X = pd.DataFrame(rows, columns=FEATURES)
    return X, ids, passthrough_rows


def main():
    wait_for_redis()
    print("Redis ready ✅", flush=True)

    ensure_group()

    print(f"Loading model from {MODEL_PATH}...", flush=True)
    model = joblib.load(MODEL_PATH)
    print(f"Model loaded ✅ ({type(model).__name__})", flush=True)

    while True:
        try:
            resp = r.xreadgroup(
                GROUP,
                CONSUMER,
                {STREAM_IN: ">"},
                count=BATCH_SIZE,
                block=BLOCK_MS,
            )

            if not resp:
                continue

            _, messages = resp[0]
            X, msg_ids, passthrough_rows = parse_batch(messages)

            if X is None:
                continue

            # ---- Inference (batch) ----
            try:
                if hasattr(model, "predict_proba"):
                    fraud_prob = model.predict_proba(X)[:, 1]
                    pred = (fraud_prob >= 0.5).astype(int)
                else:
                    pred = model.predict(X)
                    fraud_prob = None
            except Exception as e:
                print(f"❌ model inference failed on batch: {e}", flush=True)
                continue

            # ---- Output + ACK (pipeline) ----
            pipe = r.pipeline()

            if WRITE_SCORED:
                for i, msg_id in enumerate(msg_ids):
                    out_fields = {
                        "src_msg_id": msg_id,
                        "pred": str(int(pred[i])),
                    }
                    if fraud_prob is not None:
                        out_fields["fraud_prob"] = str(float(fraud_prob[i]))

                    # attach optional passthrough info for dashboard readability
                    for k, v in passthrough_rows[i].items():
                        out_fields[k] = str(v)

                    pipe.xadd(STREAM_OUT, out_fields)

            for msg_id in msg_ids:
                pipe.xack(STREAM_IN, GROUP, msg_id)

            pipe.execute()

            # ---- Lightweight log ----
            if fraud_prob is not None:
                print(
                    f"✅ batch={len(msg_ids)} | avg_prob={float(pd.Series(fraud_prob).mean()):.4f} | fraud={int(pred.sum())}",
                    flush=True,
                )
            else:
                print(f"✅ batch={len(msg_ids)} | fraud={int(pd.Series(pred).sum())}", flush=True)

        except redis.ResponseError as e:
            if "NOGROUP" in str(e):
                print("⚠️ NOGROUP: recreating group...", flush=True)
                ensure_group()
                continue
            raise


if __name__ == "__main__":
    main()
