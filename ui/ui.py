import time
from threading import Thread
from taipy.gui.builder import Page, part, layout, text, table, date, html, selector, chart, metric, menu
from taipy.gui import Gui, Icon, navigate
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import random as rand
import pandas as pd


from pages.rules import create_rule_page


UPDATE_INTERVAL = 0.5
NUM_RECENT_DETECTION = 5

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('fraud_detection')
session.row_factory = dict_factory

# Global state variables
n_trans_today = 0
fraud_count = 0
fraud_rate = 0
total_fraud_amount = 0
latest_transaction = pd.DataFrame()

#######################################################
# Overview Page
#######################################################

# Extract data from database
def get_total_transactions(day: str) -> int:
    query = f"SELECT COUNT(*) FROM predictions_by_day WHERE day = '{day}'"
    result = session.execute(query).one()
    return result['count']

def get_fraud_count(day: str) -> int:
    query = f"SELECT COUNT(*) FROM predictions_by_day WHERE day = '{day}' AND class = 1 ALLOW FILTERING"
    result = session.execute(query).one()
    return result['count']

def get_total_fraud_amount(day: str) -> float:
    query = f"SELECT SUM(amount) FROM predictions_by_day WHERE day = '{day}' AND class = 1 ALLOW FILTERING"
    result = session.execute(query).one()
    return result['system.sum(amount)']

def get_latest_rows(count: int) -> pd.DataFrame:
    query = f"SELECT * FROM predictions_by_day LIMIT {count}"
    rows = session.execute(query)
    df = pd.DataFrame(list(rows))
    if not df.empty:
        df['prediction_score'] = pd.to_numeric(df['prediction_score'], errors='coerce').round(2)
    return df


# Update data
def update_dashboard(gui: Gui, count=10, interval=0.5):
    today = "20251224" 
    global latest_transaction
    global n_trans_today
    global fraud_count
    global total_fraud_amount
    global fraud_rate
    
    while True:
        try:
            total_trans = get_total_transactions(today)
            f_count = get_fraud_count(today)
            f_amount = get_total_fraud_amount(today)
            df_recent = get_latest_rows(count)
            
            f_rate = round((f_count / total_trans * 100), 2) if total_trans > 0 else 0

            try:
                gui.broadcast_callback(lambda state: state.assign("n_trans_today", total_trans))
                gui.broadcast_callback(lambda state: state.assign("fraud_count", f_count))
                gui.broadcast_callback(lambda state: state.assign("fraud_rate", f_rate))
                gui.broadcast_callback(lambda state: state.assign("total_fraud_amount", f_amount))
                gui.broadcast_callback(lambda state: state.assign("latest_transaction", df_recent))
            except Exception as e:
                print(f"Lỗi khi cập nhật: {e}")
            
            time.sleep(interval)
        except Exception as e:
            print(f"Lỗi khi cập nhật: {e}")
            time.sleep(2)
            
            
def score_class(score: float) -> str:
    if score >= 7.5:
        return "score-high"
    elif score >= 5:
        return "score-mid-high"
    elif score >= 2.5:
        return "score-mid-low"
    else:
        return "score-low"
    
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
                text(value="### Recent Transaction", mode="md")
                
                for i in range(5):
                    condition = f"len(latest_transaction) > {i}"
                    
                    with part(class_name="item"):
                        with layout(columns="3 2"):
                            with part():
                                text(f"Event ID: {{latest_transaction.iloc[{i}]['event_id'] if {condition} else '---'}}")
                            
                            with layout(columns="1 1"):
                                with part(class_name="text-right"):
                                    text("Score:")
                                
                                with part(class_name="text-left"):
                                    text(f"{{latest_transaction.iloc[{i}]['prediction_score'] if {condition} else '0'}}", 
                                            class_name=f"score {{score_class(latest_transaction.iloc[{i}]['prediction_score']) if {condition} else ''}}")                                

menu_options = [
    ("overview", Icon("ui/static/home.png", "Overview")),
    ("rule", "Rule"),
    ("detail", "Detail")
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
rule_page = create_rule_page()

pages = {
    "/": root_page,
    "overview": overview_page,
    # "rule": rule_page,
    # "detail": detail_page
}

gui = Gui(pages=pages, css_file="static/style.css")
t = Thread(target=update_dashboard, args=(gui, NUM_RECENT_DETECTION, UPDATE_INTERVAL), daemon=True)
t.start()

gui.run(port=50001, title="Dashboard", dark_mode=False, server_config={"socketio": {"ping_interval": 1}})

