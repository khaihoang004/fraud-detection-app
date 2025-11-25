from taipy.gui.builder import Page, part, layout, text, table, input, selector, button, html
import pandas as pd
from taipy.gui import Gui, State
import re
import copy
    
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
            print("Cảnh báo: Cần nhập đầy đủ Cột và Giá trị.")
            return

        new_rule = {
            'column': state.selected_column,
            'operator': state.selected_operator,
            'value': state.rule_value
        }

        state.rules_list = state.rules_list + [new_rule]
        state.rules_df = pd.DataFrame(state.rules_list)

        state.rule_value = ""
        apply_rules(state)

    def apply_rules(state):
        df_temp = state.df.copy()

        for rule in state.rules_list:
            col = rule['column']
            op = rule['operator']
            val = str(rule['value']).strip()
            
            try:
                if op in [">", "<", ">=", "<=", "==", "!="]:
                    if df_temp[col].dtype in ['int64', 'float64']:
                        try:
                            val_typed = float(val)
                            condition = f"`{col}` {op} {val_typed}"
                            df_temp = df_temp.query(condition)
                        except ValueError:
                            print(f"Error '{val}' is not a valid number for '{col}'.")
                            continue
                    else:
                        condition = f"`{col}` {op} '{val}'"
                        df_temp = df_temp.query(condition)

                elif op == "contains":
                    df_temp = df_temp[df_temp[col].astype(str).str.contains(str(val), case=False, na=False)]
                
                elif op == "not contains":
                    df_temp = df_temp[~df_temp[col].astype(str).str.contains(str(val), case=False, na=False)]

            except Exception as e:
                print(f"Lỗi khi áp dụng quy tắc '{rule}': {e}")
                continue

        state.filtered_df = df_temp

    def reset_all(state):
        state.rules_list = []
        state.rules_df = pd.DataFrame(columns=['column', 'operator', 'value'])
        state.filtered_df = state.df.copy()
    
    def save_rules(state):
        """Lưu rules_list hiện tại vào file JSON."""
        try:
            # Ghi rules_list vào file JSON
            with open(RULES_FILENAME, 'w', encoding='utf-8') as f:
                json.dump(state.rules_list, f, indent=4)
            print(f"✅ Đã lưu {len(state.rules_list)} quy tắc vào file '{RULES_FILENAME}'.")
        except Exception as e:
            print(f"❌ Lỗi khi lưu quy tắc vào file JSON: {e}")
            
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
