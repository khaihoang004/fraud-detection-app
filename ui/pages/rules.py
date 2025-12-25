from taipy.gui.builder import Page, part, layout, text, table, input, selector, button, html
from taipy.gui import Gui, notify
import pandas as pd
import requests
import uuid

try:
    from src.common.config import RULE_API_URL
except ImportError:
    RULE_API_URL = "http://localhost:8000/update-rule"

sample_data = pd.DataFrame({
    "event_id": [str(uuid.uuid4()) for _ in range(5)],
    "Amount": [500, 0, 450, 99, 13],
    "Class": [0, 1, 0, 1, 0]
})

# 2. Khởi tạo các biến State cho Taipy
selected_column = "amount"
selected_operator = ">"
rule_value = "10000"
rules_list = []
rules_df = pd.DataFrame(columns=['column', 'operator', 'value'])
filtered_df = sample_data.copy()
OPS = [">", "<", ">=", "<=", "==", "!=", "contains"]
col_options = list(sample_data.columns)
rules_text = "Amount = 99\n Amount = 0"
def add_rule(state):
    if not state.selected_column or not state.rule_value:
        notify(state, "warning", "⚠️ Missing column or value")
        return

    # Tạo object theo đúng Schema RuleModel của FastAPI
    rule = {
        "rule_id": f"R_{state.selected_column.upper()}_{uuid.uuid4().hex[:6]}",
        "template": "condition",
        "params": {
            "field": state.selected_column,
            "op": state.selected_operator,
            "value": float(state.rule_value) if state.rule_value.replace('.','',1).isdigit() else state.rule_value
        },
        "severity": "high",
        "enabled": True
    }

    # Cập nhật danh sách rules hiển thị trên UI
    state.rules_list = state.rules_list + [rule]
    new_row = pd.DataFrame([{
        "column": rule["params"]["field"],
        "operator": rule["params"]["op"],
        "value": rule["params"]["value"]
    }])
    state.rules_df = pd.concat([state.rules_df, new_row], ignore_index=True)
    state.rule_value = ""
    notify(state, "success", "Rule added to local list!")

def reset_all(state):
    state.rules_list = []
    state.rules_df = pd.DataFrame(columns=['column', 'operator', 'value'])
    notify(state, "info", "Cleared all local rules")

def save_rules(state):
    if not state.rules_list:
        notify(state, "warning", "No rules to save!")
        return
        
    success = 0
    for rule in state.rules_list:
        try:
            # Gửi từng rule lên FastAPI
            resp = requests.post(RULE_API_URL, json=rule, timeout=5)
            if resp.status_code == 200:
                success += 1
            else:
                print(f"Failed: {resp.text}")
        except Exception as e:
            notify(state, "error", f"API connection error: {e}")
            break

    notify(state, "success", f"✅ Successfully deployed {success} rules to Kafka!")

def create_rule_page():
    page = Page()
    with page:
        text("## Rule Overlay", mode="md")
        
        with layout("1 2"):
            with part():
                with part(class_name="card"):
                    text("#### Add New Rule", mode="md")
                    with layout("1 1 1"):
                        selector(label="Field", value="{selected_column}", lov="{col_options}", dropdown=True)
                        selector(label="Operator", value="{selected_operator}", lov="{OPS}", dropdown=True)
                        input(label="Value", value="{rule_value}")
                    
                    button(label="➕ Add Rule", on_action=add_rule, class_name="fullwidth")
                
                with part(class_name="card"):
                    text("#### Pending Deployment", mode="md")

                    text(
                        value="Amount = 99"
                    )
                    text(
                        value="Amount = 0"
                    )

                    button(
                        label="🚀 Deploy Rules",
                        on_action=save_rules,
                        class_name="primary fullwidth"
                    )
            with part():
                with part(class_name="card"):
                    text("#### Preview", mode="md")
                    table(data="{filtered_df}", page_size=6)
                
                with part(class_name="spacer"):
                    pass
                
                with layout("1 1"):
                    with part(class_name="card"):
                        text(value="Total Fraud Cases", class_name="card-title")
                        text(value="{len(filtered_df[filtered_df['Class']==1])}", class_name="card-amount")
                    with part(class_name="card"):
                        text(value="Potential Loss", class_name="card-title")
                        text(value="${sum(filtered_df[filtered_df['Class']==1]['Amount'])}", class_name="card-amount")

    return page

if __name__ == "__main__":
    app_gui = Gui(page=create_rule_page(), css_file="static/style.css")
    app_gui.run(
        title="Fraud Control Center",
        port=5004,
        dark_mode=False,
        use_reloader=True
    )