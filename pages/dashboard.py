from taipy.gui.builder import Page, part, layout, text, table, date, html, selector, chart, metric, menu
import pandas as pd
import random as rand

def random_score():
    return round(rand.random() * 10, 2)

def score_class(score: float) -> str:
    if score >= 7.5:
        return "score-high"
    elif score >= 5:
        return "score-mid-high"
    elif score >= 2.5:
        return "score-mid-low"
    else:
        return "score-low"

def get_total_transaction(mode: str):
    if mode == "1d":
        return 1000
    if mode == "3d":
        return 3000
    if mode == "7d":
        return 7000
    else:
        raise ValueError(f"Invalid mode: {mode}")

def get_num_fraud(mode: str):
    if mode == "1d":
        return rand.randint(0, 10)
    if mode == "3d":
        return rand.randint(0, 30)
    if mode == "7d":
        return rand.randint(0, 70)
    else:
        raise ValueError(f"Invalid mode: {mode}")

def change_time_range(state):
    state.suboptions = ["1d", "3d", "7d"]
    state.selected_suboption = state.suboptions[0]

def create_dashboard_page(data):
    page = Page()
    
    options = ["1d", "3d", "7d"]
    selected_option = options[0]
    
    scores = [(f"Case {i}", random_score()) for i in range (100)]
    scores_df = pd.DataFrame(scores, columns=["Case_ID", "Score"])
    
    detection_count = {
        'detected_by': [
            'custom rule', 'model_A', 'model_B'
        ],
        'count': [
            rand.randint(50, 300) for _ in range(3)
        ]
    }

    detection_count_df = pd.DataFrame(detection_count)

    fraud_rate = 80
    delta = 15
    threshold = 50
    with page:        
        with part(class_name="topbar"):
            text(value="Fraud Detection Dashboard", class_name="topbar-text")
        
            with part(class_name="topbar-selector"): 
                selector(
                    value="{selected_option}",
                    lov=options,
                    on_change=change_time_range,
                    dropdown=True,
                )

        with part(class_name="content"):
            # with layout("1 1"):
                # with part(class_name="metric"):
                #     metric("{fraud_rate}", 
                #             delta="{delta}", 
                #             threshold="{threshold}",
                #             width = "100%")
                    
                #     # Thay thành pie chart: sus, good, uncategorized
                
            
            with layout("1 1 1 1"):
                with part(class_name="card"):
                    text(value="Total transactions", class_name="card-title")
                    text(value=get_total_transaction(mode="1d"), class_name="card-amount")
                with part(class_name="card"):
                    text(value="Total frauds", class_name="card-title")
                    text(value=get_num_fraud(mode="1d"), class_name="card-amount")
                with part(class_name="card"):
                    pass
                with part(class_name="card"):
                    text(value="Custom Rules", class_name="card-title")
                    text(value=13, class_name="card-amount")
        

            
            html("br")
            with layout(columns="1 1"):
                with part(class_name="recent-detection"):
                    text(value="### Recent Detection (xxx)", mode="md" )
                    
                    for i in range (5):
                        name, score = scores[i]
                        with part(class_name="item"):
                            with layout(columns="3 2"):
                                
                                with part():
                                    text(value=name)
                                    
                                with layout(columns="1 1"):
                                    with part(class_name="text-right"):
                                        text(value=f"Score:")
                                        
                                    with part(class_name="text-left"):
                                        text(f"{score}", class_name=f"score {score_class(score)}")
                    
                with part(class_name="detector-count"):
                    text(value="### Detections by Detector", mode="md")

                    num_bars = len(detection_count_df)
                    bar_height_px = 80
                    gap_px = 40

                    total_height = num_bars * (bar_height_px + gap_px)
                    
                    properties = {
                        "x": "count",
                        "y": "detected_by",

                        "color": "#ff6049",
                        "textposition": "outside",
                        "height": "{total_height}",
                        "width": "40vw",
                        "orientation": "h",
                        "layout": {
                            "xaxis": {"title": "", 
                                      "showticklabels": False,
                                      },
                            "yaxis": {"title": ""},
                            
                            "showlegend": False,
                            "margin": {"l": 100},
                            "hovermode": False,
                            "dragmode": False,
                        },
                        "config": {
                            "displayModeBar": False,
                            'displaylogo': False,
                        }
                    }
                    
                    chart(
                        data="{detection_count_df}",
                        type="bar",
                        text="count",
                        properties="{properties}",
                    )

            # with layout(columns="2 1"):
            #     with part():
            #         text("Filter **From**", mode="md")
            #         date("{start_date}")
            #         text("To")
            #         date("{end_date}")

            # html("br")
            # with part(class_name="table"):
            #     text(value="### Recent Cases", mode="md")
            #     recent_case = data.sort_values(by="Time", ascending=False).head(10)
            #     recent_case = recent_case[["Time", "Amount", "Class"]]
            #     table(data="{recent_case}", page_size=10)


    return page
