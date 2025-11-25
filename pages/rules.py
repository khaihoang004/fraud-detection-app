from taipy.gui.builder import Page, part, layout, text, table, date, html, selector, chart, metric, menu
import pandas as pd
import random as rand

def create_rule_page(data):
    page = Page()
    with page:
        with part(class_name="table"):
            text(value="### Recent Cases", mode="md")
            recent_case = data.sort_values(by="Time", ascending=False).head(10)
            recent_case = recent_case[["Time", "Amount", "Class"]]
            table(data="{recent_case}", page_size=10)
                
    return page