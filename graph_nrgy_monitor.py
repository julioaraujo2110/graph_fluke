import streamlit as st
import pandas as pd
import altair as alt

st.sidebar.image("assets/logo.png")

st.title("Datos del analizador")

uploaded_file = st.file_uploader("Agrega tu archivo .csv", type="csv")

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)

    # Convert 'Timestamp' to datetime objects
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df.set_index('Timestamp')

    st.sidebar.header("Opciones de visualización")

    # Date Range Selection
    min_date = df.index.min()
    max_date = df.index.max()
    start_date = st.sidebar.date_input("Start Date", min_date)
    end_date = st.sidebar.date_input("End Date", max_date)

    # Time Range Selection (optional, for more granular control)
    start_time = st.sidebar.time_input("Start Time", df.index.min().time())
    end_time = st.sidebar.time_input("End Time", df.index.max().time())

    start_datetime = pd.to_datetime(f"{start_date} {start_time}")
    end_datetime = pd.to_datetime(f"{end_date} {end_time}")

    filtered_df = df[(df.index >= start_datetime) & (df.index <= end_datetime)]

    # Parameter Selection
    parameter_options = filtered_df.columns.tolist()
    selected_parameter = st.sidebar.selectbox("Select Parameter to Plot", parameter_options)

    st.subheader(f"Dynamic Plot of {selected_parameter}")

    if selected_parameter:
        try:
            chart_data = filtered_df[[selected_parameter]].reset_index()
            chart = alt.Chart(chart_data).mark_line().encode(
                x=alt.X('Timestamp', title='Time'),
                y=alt.Y(selected_parameter, title=selected_parameter),
                tooltip=['Timestamp', selected_parameter]
            ).interactive()
            st.altair_chart(chart, use_container_width=True)
        except KeyError:
            st.error("Selected parameter not found in the data.")
        except Exception as e:
            st.error(f"An error occurred while plotting: {e}")
    else:
        st.info("Please select a parameter to plot.")