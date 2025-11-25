from taipy.gui.builder import Page, part, layout, text, table, date, html, selector, toggle, button, expandable, input
import pandas as pd

def create_detail_page(data: pd.DataFrame):
    initial_data = data.copy()

    if initial_data["Time"].dtype in ['int64', 'float64']:
         initial_data['Date'] = pd.to_datetime(initial_data['Time'], unit='s').dt.date
    else:
         initial_data['Date'] = pd.to_datetime(initial_data['Time']).dt.date    
    
    min_time_date = initial_data['Date'].min()
    max_time_date = initial_data['Date'].max()
    
    start_date_filter = str(min_time_date)
    end_date_filter = str(max_time_date)
    
    min_amount_filter = initial_data["Amount"].min()
    max_amount_filter = initial_data["Amount"].max()
    
    sort_column = "Time"
    sort_order = True # True = Descending, False = Ascending
    sort_order_lov = [("Descending", True), ("Ascending", False)]
    
    v_cols = [f"V{i}" for i in range(1, 29)]
    selected_v_cols = ["Time", "Amount", "Class", "V13", "V17"]

    all_display_cols = ["Time", "Amount", "Class"] + v_cols
    
    display_cols_initial = [col for col in selected_v_cols if col in initial_data.columns]
    filtered_data = initial_data[display_cols_initial].sort_values(by="Time", ascending=False).head(10)

    def apply_filters(state):
        df = initial_data.copy()
        
        start_date = pd.to_datetime(state.start_date_filter).date()
        end_date = pd.to_datetime(state.end_date_filter).date()

        df = df[
            (df['Date'] >= start_date) &
            (df['Date'] <= end_date)
        ]
        
        try:
            min_amount = float(state.min_amount_filter)
            max_amount = float(state.max_amount_filter)
        except ValueError:
            print("Warning: Amount filter input is invalid (not a number). Using original min/max.")
            min_amount = initial_data["Amount"].min()
            max_amount = initial_data["Amount"].max()
            
        df = df[
            (df['Amount'] >= min_amount) &
            (df['Amount'] <= max_amount)
        ]
        
        ascending = not state.sort_order 
        col_to_sort = state.sort_column if state.sort_column in df.columns else "Time"
        df = df.sort_values(by=col_to_sort, ascending=ascending)
        
        display_cols = [col for col in state.selected_v_cols if col in df.columns]

        state.filtered_data = df[display_cols]

        print(f"Applied filter: {len(state.filtered_data)} rows remaining.")
            
    page = Page()
    with page:
        with part(class_name="topbar"):
            text("Detailed Fraud Case Analysis", class_name="topbar-text")

        with expandable("Filter", expanded=False, class_name="filter"):
            with layout(columns="1 1 1 1"):
                text("Time", mode="md")
                
                with layout("1 2"):
                    text("From", mode="md")
                    date("{start_date_filter}", on_change=apply_filters)
                with layout("1 2"):
                    text("to", mode="md")
                    date("{end_date_filter}", on_change=apply_filters)

            with layout(columns="1 1 1 1"):
                text("Amount", mode="md")

                with layout("1 2"):
                    text("From", mode="md")
                    input(value="{min_amount_filter}", on_change=apply_filters)
                with layout("1 2"):
                    text("to", mode="md")
                    input(value="{max_amount_filter}", on_change=apply_filters)

            text("Select **Columns** to Display", mode="md")
            selector(
                value="{selected_v_cols}",
                lov=all_display_cols,
                multiple=True,
                dropdown=True,
                on_change=apply_filters
            )
            
            # button("Apply Filters", on_action=apply_filters)
        html("br")
        with part(class_name="table-container"):
            table(
                data="{filtered_data}", 
                page_size=10, 
                width="100%",
                columns={col: {'title': col} for col in all_display_cols} # Đặt tên cột
            )

    return page