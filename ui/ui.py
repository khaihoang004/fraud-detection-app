import time
from threading import Thread
from taipy.gui.builder import Page, part, layout, text, table, date, html, selector, chart, metric, menu, button, toggle, input
from taipy.gui import Gui, Icon, navigate, notify
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import pandas as pd
import os
import json

UPDATE_INTERVAL = 0.5
NUM_RECENT_TRANSACTION = 10
NUM_TOP_FRAUD = 10

# Connect to Cassandra
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")

cluster = Cluster([CASSANDRA_HOST], port=9042)
session = cluster.connect('fraud_detection')
session.row_factory = dict_factory

# UI server
UI_HOST = os.getenv("UI_HOST", "0.0.0.0")
UI_PORT = int(os.getenv("UI_PORT", 5002))

# Rules
RULE_FILE = "rules.json"

#######################################################
# Overview Page
#######################################################

# Extract data from database
def get_total_transactions(day: str) -> int:
    query = f"SELECT COUNT(*) FROM predictions_by_day_asc WHERE day = '{day}'"
    result = session.execute(query).one()
    return result['count']

def get_fraud_count(day: str) -> int:
    query = f"""
    SELECT COUNT(*) 
    FROM predictions_by_day_asc 
    WHERE day = '{day}' AND class = 'Fraud'
    ALLOW FILTERING
    """
    result = session.execute(query).one()
    return result['count']

def get_fraud_suspicious_count_by_hour(day: str) -> pd.DataFrame:
    """
    Lấy số lượng Fraud và Suspicious theo từng giờ trong ngày.

    Output DataFrame:
    | hour | fraud_count | suspicious_count |
    """
    query = f"""
    SELECT event_ts, class
    FROM predictions_by_day_asc
    WHERE day = '{day}'
    """
    rows = session.execute(query)
    df = pd.DataFrame(list(rows))

    if df.empty:
        return pd.DataFrame(
            columns=["hour", "fraud_count", "suspicious_count"]
        )

    df["event_ts"] = pd.to_datetime(df["event_ts"])

    # Lấy giờ
    df["hour"] = df["event_ts"].dt.hour

    # Đếm theo giờ & class
    count_df = (
        df.groupby(["hour", "class"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    # Đảm bảo luôn có đủ cột
    if "Fraud" not in count_df.columns:
        count_df["Fraud"] = 0
    if "Suspicious" not in count_df.columns:
        count_df["Suspicious"] = 0

    count_df = count_df.rename(
        columns={
            "Fraud": "fraud_count",
            "Suspicious": "suspicious_count"
        }
    )

    # Đảm bảo đủ 24 giờ
    count_df = (
        pd.DataFrame({"hour": range(24)})
        .merge(count_df, on="hour", how="left")
        .fillna(0)
    )

    return count_df

def get_total_fraud_amount(day: str) -> float:
    query = f"""
    SELECT SUM(amount) 
    FROM predictions_by_day_asc
    WHERE day = '{day}' AND class = 'Fraud'
    ALLOW FILTERING
    """
    result = session.execute(query).one()
    return result['system.sum(amount)']

def get_latest_transaction(count: int, day) -> pd.DataFrame:
    query = f"""
    SELECT * 
    FROM predictions_by_day_asc
    WHERE day='{day}'
    ORDER BY event_ts DESC
    LIMIT {count}"""
    rows = session.execute(query)
    df = pd.DataFrame(list(rows))
    if not df.empty:
        df['prediction_score'] = pd.to_numeric(df['prediction_score'], errors='coerce').round(4)
    return df

def get_avg_score_trend(time_unit="hour", day="20251224"):
    query = f"SELECT event_ts, prediction_score FROM predictions_by_day_asc WHERE day='{day}'"
    rows = session.execute(query)
    df = pd.DataFrame(list(rows))
    
    if df.empty:
        return pd.DataFrame(columns=["time", "avg_score"])
    
    df['event_ts'] = pd.to_datetime(df['event_ts'])
    df['prediction_score'] = pd.to_numeric(df['prediction_score'], errors='coerce')
    
    if time_unit == "hour":
        df['time'] = df['event_ts'].dt.hour
    else:
        df['time'] = df['event_ts'].dt.date
    
    trend_df = df.groupby('time')['prediction_score'].mean().reset_index()
    trend_df.rename(columns={'prediction_score': 'avg_score'}, inplace=True)
    
    return trend_df

def get_top_fraud(n=10, day="20251224"):
    """
    Lấy top n giao dịch nghi ngờ
    """
    query = f"""
    SELECT event_ts, event_id, amount, prediction_score, class
    FROM predictions_by_day_asc
    WHERE day='{day}' AND class='Fraud'
    ALLOW FILTERING
    """
    rows = session.execute(query)
    df = pd.DataFrame(list(rows))
    if df.empty:
        return pd.DataFrame(columns=["event_ts", "event_id", "amount", "prediction_score"])
    
    df['prediction_score'] = pd.to_numeric(df['prediction_score'], errors='coerce').round(2)
    df = df.sort_values(by="prediction_score", ascending=False).head(n)
    
    return df

def get_today() -> str:
    return time.strftime("%Y%m%d")

# Global state variables
today = get_today()
n_trans_today = 0
fraud_count = 0
fraud_rate = 0
total_fraud_amount = 0
latest_transaction = get_latest_transaction(NUM_RECENT_TRANSACTION, today)
score_trend_df = get_avg_score_trend(day=today)
top_fraud = get_top_fraud(n=NUM_TOP_FRAUD, day=today)
print("___TOP FRAUD INITIAL___")
print(top_fraud)
# total_alert_df  = pd.DataFrame()
rules_overlay_enabled = False
rules = []

# Update data
def update_dashboard(gui: Gui, count=10, interval=0.5):
    today = get_today()
    global latest_transaction
    global n_trans_today
    global fraud_count
    global total_fraud_amount
    global fraud_rate
    global top_fraud
    global rules
    
    while True:
        try:
            total_trans = get_total_transactions(today)
            f_count = get_fraud_count(today)
            f_amount = get_total_fraud_amount(today)
            df_recent = get_latest_transaction(count, today)
            df_top_fraud = get_top_fraud(count, today)
            f_rate = round((f_count / total_trans * 100), 2) if total_trans > 0 else 0
            
            rules = load_rules()
            df_recent = apply_rules_to_df(df_recent, rules)
            df_top_fraud = apply_rules_to_df(df_top_fraud, rules)

            try:
                gui.broadcast_callback(lambda state: state.assign("n_trans_today", total_trans))
                gui.broadcast_callback(lambda state: state.assign("fraud_count", f_count))
                gui.broadcast_callback(lambda state: state.assign("fraud_rate", f_rate))
                gui.broadcast_callback(lambda state: state.assign("total_fraud_amount", f_amount))
                gui.broadcast_callback(lambda state: state.assign("latest_transaction", df_recent))
                gui.broadcast_callback(lambda state: state.assign("top_fraud", df_top_fraud))
            except Exception as e:
                print(f"Lỗi khi cập nhật: {e}")
            
            time.sleep(interval)
        except Exception as e:
            print(f"Lỗi khi cập nhật: {e}")
            time.sleep(2)
            
def update_score_trend(gui: Gui, interval=900, time_unit="hour"):
    global score_trend_df
    while True:
        try:
            trend_df = get_avg_score_trend(time_unit)
            gui.broadcast_callback(lambda state: state.assign("score_trend_df", trend_df))
            time.sleep(interval)
        except Exception as e:
            print(f"Lỗi khi lấy trend prediction_score: {e}")
            time.sleep(5)

# def update_alert_by_hour(gui: Gui, interval=2):
#     global total_alert_df
#     today = "20251225" 
    
#     while True:
#         try:
#             alert_df = get_fraud_suspicious_count_by_hour(today)
#             gui.broadcast_callback(lambda state: state.assign("total_alert_df", alert_df))
#             time.sleep(interval)
#         except Exception as e:
#             print(f"Lỗi khi lấy trend prediction_score: {e}")
#             time.sleep(5)

                      
def score_class(score) -> str:
    if float(score) >= 0.8:
        return "score-high"
    elif float(score) >= 0.15:
        return "score-mid"
    else:
        return "score-low"

def load_rules() -> list:
    if not os.path.exists(RULE_FILE):
        return []
    with open(RULE_FILE) as f:
        return json.load(f)

def get_rules(state):
    """
    Cache rules trong state để tránh load lại mỗi lần render
    """
    if not hasattr(state, "rules_cache"):
        state.rules_cache = load_rules()
    return state.rules_cache

def check_rule(row: dict, rule: dict) -> bool:
    """
    Check 1 rule với 1 row (row là dict)
    """
    if not rule.get("enabled", True):
        return False

    field = rule.get("field")
    op = rule.get("op")
    value = rule.get("value")

    if field not in row:
        return False

    val = row[field]

    try:
        if op == "==":
            match = val == value
        elif op == "!=":
            match = val != value
        elif op == ">":
            match = val > value
        elif op == "<":
            match = val < value
        elif op == ">=":
            match = val >= value
        elif op == "<=":
            match = val <= value
        else:
            return False
    except Exception:
        return False

    # AND condition (nested rule)
    if match and "and" in rule:
        return check_rule(row, rule["and"])

    return match

def match_any_rule(row: dict, rules: list) -> bool:
    """
    True nếu row match ít nhất 1 rule
    """
    for rule in rules:
        if check_rule(row, rule):
            return True
    return False

def apply_rules_to_df(df: pd.DataFrame, rules: list) -> pd.DataFrame:
    """
    Thêm cột:
    - rule_match: bool (row có match rule hay không)
    """
    if df.empty:
        return df

    df = df.copy()

    df["rule_match"] = df.apply(
        lambda r: match_any_rule(r.to_dict(), rules),
        axis=1
    )               
    return df

def on_toggle_rules(state):
    state.rules_overlay_enabled = state.rules_overlay_enabled

def highlight_rule(rule_match):
    return "highlight-rule" if rule_match else ""

with Page() as overview_page:
    with part(class_name="topbar"):
        text(value="Fraud Detection Dashboard", class_name="topbar-text")

    with part(class_name="content"):
        with layout("1 1 1 1"):
            with part(class_name="card"):
                text(value="Total transactions", class_name="card-title")
                text(value="{n_trans_today}", class_name="card-amount")
                
            with part(class_name="card"):
                text(value="Detected Frauds", class_name="card-title")
                text(value="{fraud_count}", class_name="card-amount status-fraud")
            
            with part(class_name="card"):
                text(value="Fraud Rate (%)", class_name="card-title")
                text(value="{fraud_rate}", class_name="card-amount")
                
            with part(class_name="card"):
                text(value="Total Fraud Value", class_name="card-title")
                text(value="${f'{total_fraud_amount:,.0f}'}", class_name="card-amount status-value")    
                    
        with layout(columns="1 1"):
            with part(class_name="recent-transaction"):
                with layout(columns="2 1"):
                    text(value="### Top fraud", mode="md")

                    toggle(
                        value="{rules_overlay_enabled}",
                        label="Rules Overlay",
                        on_change=on_toggle_rules,
                    )

                num_items = min(len(top_fraud), NUM_TOP_FRAUD)

                if num_items == 0:
                    text(f"#### -- No fraud detected --", mode="md")
                
                for i in range(NUM_TOP_FRAUD):
                    condition = f"len(top_fraud) > {i}"
                    
                    # VIEW while OVERLAY DISABLED
                    with part(render="{not rules_overlay_enabled}", class_name="item"):
                        with layout(columns="3 2"):
                            with part():
                                text(f"Event ID: {{top_fraud.iloc[{i}]['event_id'] if {condition} else '---'}}")
                                
                            with layout(columns="1 1"):
                                with part(class_name="text-right"):
                                    text("Score:")
                            
                                with part(class_name="text-left"):
                                    text(f"{{top_fraud.iloc[{i}]['prediction_score'] if {condition} else '0'}}",
                                        class_name=f"score {{score_class(top_fraud.iloc[{i}]['prediction_score']) if {condition} else ''}}")

                    # VIEW while OVERLAY ENABLED
                    with part(render="{rules_overlay_enabled}", class_name=f"item {{highlight_rule(top_fraud.iloc[{i}]['rule_match']) if {i} < num_items else ''}}"):
                        with layout(columns="3 2"):
                            with part():
                                text(f"Event ID: {{top_fraud.iloc[{i}]['event_id'] if {condition} else '---'}}")
                            with layout(columns="1 1"):
                                with part(class_name="text-right"):
                                    text("Score:")
                                
                                with part(class_name="text-left"):
                                    text(f"{{top_fraud.iloc[{i}]['prediction_score'] if {condition} else '0'}}",
                                        class_name=f"score {{score_class(top_fraud.iloc[{i}]['prediction_score']) if {condition} else ''}}")

            with part(class_name="recent-transaction"):
                text(value="### Average Fraud Prediction Score over hours", mode="md")
                chart(
                    data="{score_trend_df}",
                    x="time",
                    y="avg_score",
                    type="line"
                )

        with part(class_name="recent-transaction"):
            text(value="### Recent Transaction", mode="md")
            for i in range(NUM_RECENT_TRANSACTION):
                condition = f"len(latest_transaction) > {i}"
                
                # VIEW while OVERLAY DISABLED
                with part(render="{not rules_overlay_enabled}", class_name="item"):
                    with layout(columns="3 2"):
                        with part():
                            text(f"Event ID: {{latest_transaction.iloc[{i}]['event_id'] if {condition} else '---'}}")
                        
                        with layout(columns="1 1"):
                            with part(class_name="text-right"):
                                text("Score:")
                            
                            with part(class_name="text-left"):
                                text(f"{{latest_transaction.iloc[{i}]['prediction_score'] if {condition} else '0'}}", 
                                        class_name=f"score {{score_class(latest_transaction.iloc[{i}]['prediction_score']) if {condition} else ''}}")                                
                
                # VIEW while OVERLAY ENABLED
                with part(render="{rules_overlay_enabled}", class_name=f"item {{highlight_rule(latest_transaction.iloc[{i}]['rule_match'])}}"):
                    with layout(columns="3 2"):
                        with part():
                            text(f"Event ID: {{latest_transaction.iloc[{i}]['event_id'] if {condition} else '---'}}")
                        
                        with layout(columns="1 1"):
                            with part(class_name="text-right"):
                                text("Score:")
                            
                            with part(class_name="text-left"):
                                text(f"{{latest_transaction.iloc[{i}]['prediction_score'] if {condition} else '0'}}", 
                                        class_name=f"score {{score_class(latest_transaction.iloc[{i}]['prediction_score']) if {condition} else ''}}")

#######################################################
# Rule Page
#######################################################
def add_rule(state):
    if not state.selected_field or not state.rule_value.strip():
        notify(state, "warning", "Missing field or value")
        return

    try:
        value = float(state.rule_value)
    except ValueError:
        value = state.rule_value

    rule = {
        "field": state.selected_field,
        "op": state.selected_op,
        "value": value,
        "enabled": True
    }

    state.rules_list.append(rule)
    state.rules_list = state.rules_list[:]

    save_rules(state.rules_list)

    all_df = get_all_transactions_for_preview(today)
    state.preview_df = apply_rules_to_df(all_df, state.rules_list)

    sync_rules_df(state)
    
    state.rule_value = ""
    notify(state, "success", "Rule added")


def get_all_transactions_for_preview(day="20251224") -> pd.DataFrame:
    """
    Lấy toàn bộ giao dịch trong ngày để preview rule
    """
    query = f"""
    SELECT event_ts, event_id, amount, prediction_score, class
    FROM predictions_by_day_asc
    WHERE day='{day}'
    """
    rows = session.execute(query)
    df = pd.DataFrame(list(rows))

    if df.empty:
        return pd.DataFrame(
            columns=["event_ts", "event_id", "amount", "prediction_score", "class"]
        )

    df["prediction_score"] = pd.to_numeric(
        df["prediction_score"], errors="coerce"
    ).round(4)

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    return df


def get_rule_preview_df(day="20251224"):
    df = get_all_transactions_for_preview(day)
    rules = load_rules()
    return apply_rules_to_df(all_df, rules)


def save_rules(rules):
    with open(RULE_FILE, "w") as f:
        json.dump(rules, f, indent=2)
        
def remove_all_rules(state):
    if not state.rules_list:
        notify(state, "info", "No rules to remove")
        return

    # Clear rules
    state.rules_list = []
    save_rules(state.rules_list)

    # Update preview
    all_df = get_all_transactions_for_preview(today)
    state.preview_df = apply_rules_to_df(all_df, state.rules_list)

    # Sync dataframe
    sync_rules_df(state)
    
    state.selected_rule = None
    notify(state, "success", "All rules removed")

def toggle_rule(state, idx):
    state.rules_list[idx]["enabled"] = not state.rules_list[idx]["enabled"]
    state.rules_list = state.rules_list[:]
    save_rules(state.rules_list)

    all_df = get_all_transactions_for_preview(today)
    state.preview_df = apply_rules_to_df(all_df, state.rules_list)

def get_matched_fraud(df):
    df = df[(df["class"] == "Fraud") & (df["rule_match"] == True)]
    return len(df)

def sync_rules_df(state):
    state.rules_df = pd.DataFrame(state.rules_list)
    num_fraud = get_matched_fraud(state.preview_df)
    print(num_fraud)
    state.matched_fraud = num_fraud

# Global state variables
selected_field = "amount"
selected_op = ">"
rule_value = " "
rules_list = load_rules()
rules_df = pd.DataFrame(rules_list)

all_df = get_all_transactions_for_preview(today)
preview_df = apply_rules_to_df(all_df, rules_list)
matched_fraud = get_matched_fraud(preview_df)

selected_rule = None

FIELDS = list(all_df.columns)
OPS = ["==", "!=", ">", "<", ">=", "<="]

with Page() as rule_page:
    with part(class_name="topbar"):
        text(value="Fraud Detection Dashboard", class_name="topbar-text")

    with part(class_name="content"):
        text("## Rule Management", mode="md")
        with layout("1 2"):
            # ===============================
            # LEFT: Rule editor
            # ===============================
            with part():
                with part(class_name="card"):
                    text("### Add New Rule", mode="md")

                    with layout("1 1 1"):
                        selector("Field", value="{selected_field}", lov="{FIELDS}", dropdown=True)
                        selector("Operator", value="{selected_op}", lov="{OPS}", dropdown=True)
                        input("Value", value="{rule_value}")

                    button("➕ Add Rule", on_action=add_rule, class_name="fullwidth")

                    with part(class_name="card"):
                        text("### Existing Rules", mode="md")

                        table(
                            data="{rules_df}",
                            page_size=6
                        )

                    button(
                        "Remove All Rules",
                        on_action=remove_all_rules,
                        class_name="fullwidth"
                    )

            # ===============================
            # RIGHT: Preview
            # ===============================
            with part():
                with part(class_name="card"):
                    text("### Preview (rule_match)", mode="md")
                    table(data="{preview_df}", page_size=6)

                with layout("1 1 1"):
                    with part(class_name="card"):
                        text("Matched Rows", class_name="card-title")
                        text(
                            value="{preview_df['rule_match'].sum()}",
                            class_name="card-amount"
                        )
                    with part(class_name="card"):
                        text("Total Rows", class_name="card-title")
                        text(
                            value="{len(preview_df)}",
                            class_name="card-amount"
                        )
                    with part(class_name="card"):
                        text("Matched Fraud", class_name="card-title")
                        text(
                            value="{matched_fraud}",
                            class_name="card-amount"
                        )
                    
menu_options = [
    ("overview", Icon("static/style.css", "Overview")),
    ("rule", "Rule"),
    # ("detail", "Detail")
]

def on_menu_action(state, action, info):
    page = info["args"][0]
    navigate(state, to=page)

root_page = Page()

with root_page:
    menu(
        # label="Options",
        lov=menu_options, 
        width="300px",
        on_action=on_menu_action
        )
    # content("{active_page}")

pages = {
    "/": root_page,
    "rule": rule_page,
    "overview": overview_page,
    # "detail": detail_page
}

gui = Gui(pages=pages, css_file="static/style.css")
t = Thread(target=update_dashboard, args=(gui, NUM_RECENT_TRANSACTION, UPDATE_INTERVAL), daemon=True)
t.start()

t1 = Thread(target=update_score_trend, args=(gui, UPDATE_INTERVAL), daemon=True)
t1.start()

# t2 = Thread(target=update_alert_by_hour, args=(gui, UPDATE_INTERVAL), daemon=True)
# t2.start()

gui.run(
    host=UI_HOST,
    port=UI_PORT,
    title="Dashboard",
    dark_mode=False,
    server_config={"socketio": {"ping_interval": 1}}
)