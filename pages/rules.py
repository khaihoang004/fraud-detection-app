from taipy.gui.builder import Page, part, layout, text, table, input, selector, button
from taipy.gui import State
import pandas as pd
import requests
import uuid

RULE_API_URL = "http://localhost:8000/rules"

def create_rule_page(data):
    OPS = [">", "<", ">=", "<=", "==", "!=", "contains", "not contains"]
    RULES_FILENAME = r"data\custom_rules.json"
    
    df = data.copy()
    filtered_df = data.copy()
    rules_list = []
    rules_df = pd.DataFrame(columns=['column', 'operator', 'value'])
    
    col_options = list(data.columns)
    selected_column = col_options[0] if col_options else ""
    selected_operator = OPS[0]
    rule_value = ""
    
    def add_rule(state):
        if not state.selected_column or not state.rule_value:
            print("⚠️ Missing column or value")
            return

        rule = {
            "rule_id": f"R_{state.selected_column.upper()}_{uuid.uuid4().hex[:6]}",
            "template": "condition",
            "params": {
                "field": state.selected_column,
                "op": state.selected_operator,
                "value": state.rule_value
            },
            "severity": "high",
            "enabled": True
        }

        state.rules_list = state.rules_list + [rule]
        state.rules_df = pd.DataFrame([
            {
                "column": r["params"]["field"],
                "operator": r["params"]["op"],
                "value": r["params"]["value"]
            }
            for r in state.rules_list
        ])

        state.rule_value = ""

    def reset_all(state):
        state.rules_list = []
        state.rules_df = pd.DataFrame(columns=['column', 'operator', 'value'])

    def save_rules(state):
        success = 0
        for rule in state.rules_list:
            try:
                resp = requests.post(RULE_API_URL, json=rule, timeout=3)
                if resp.status_code == 200:
                    success += 1
                else:
                    print(f"Failed to save rule {rule['rule_id']}: {resp.text}")
            except Exception as e:
                print(f"API error: {e}")

        print(f"✅ Sent {success}/{len(state.rules_list)} rules to Rule Service")


    page = Page()
    with page:
        
        with layout("1 2"):
            with part():
                with part(class_name="card"):
                    with layout("2 1 3"):
                        selector(label="Select Column", value="{selected_column}", lov="{col_options}", dropdown=True)
                        selector(label="Select Operator", value="{selected_operator}", lov="{OPS}", dropdown=True)
                        input(label="Input value", value="{rule_value}")
                    
                    button(label="➕ Add Rule", on_action="add_rule")
                with part(class_name="card"):
                    text(value="### Used Rules", mode="md")
                    table(data="{rules_df}", height="200px", page_size=5)
                    with layout("1 1"):
                        button(label="🗑️ Remove All Rules", on_action="reset_all")
                        button(label="💾 Save Rules", on_action="save_rules")


            with part(class_name="card"):
                table(data="{filtered_df}", page_size=10, width="100%")

        with layout("1 1 1"):
            with part(class_name="card"):
                text(value="Number of frauds", class_name="card-title")
                text(value=len(filtered_df[filtered_df["Class"]==1]), class_name="card-amount")
            with part(class_name="card"):
                text(value="Total money lost", class_name="card-title")
                text(value=sum(filtered_df[filtered_df["Class"]==1]["Amount"]),class_name="card-amount")    
    
    
    return page
