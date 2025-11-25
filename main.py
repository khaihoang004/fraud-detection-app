from taipy.gui import Gui, Icon, navigate
import pandas as pd
from pages.dashboard import create_dashboard_page
from pages.home import create_home_page
from pages.rules import create_rule_page
from pages.details import create_detail_page
from taipy.gui.builder import Page, menu, content

data = pd.read_csv("data/creditcard_data.csv")
v_cols = [f"V{i}" for i in range(1, 29)]

data[v_cols] = data[v_cols].round(3)

dashboard_page = create_dashboard_page(data)
rule_page = create_rule_page(data)
detail_page = create_detail_page(data)
test_page = Page()

menu_options = [
    ("dashboard", Icon("static/home.png", "Dashboard")),
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
    
pages = {
    "/": root_page,
    "dashboard": dashboard_page,
    "rule": rule_page,
    "detail": detail_page
}
Gui(pages=pages, css_file="static/style.css").run(port=5002, title="Dashboard", use_reloader=True, dark_mode=True)
