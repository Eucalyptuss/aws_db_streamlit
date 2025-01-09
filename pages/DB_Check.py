import streamlit as st
import pandas as pd
import pymysql
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

# 환경변수 로드
load_dotenv()

# 환경변수에서 AWS RDS 정보 가져오기
DB_HOST = os.getenv("DB_HOST")  # AWS RDS 엔드포인트
DB_USER = os.getenv("DB_USER")  # 사용자 이름
DB_PASSWORD = os.getenv("DB_PASSWORD")  # 비밀번호
DB_NAME = os.getenv("DB_NAME")  # 데이터베이스 이름

# DB 연결 함수
def get_rds_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

# DB 데이터 조회 함수
def fetch_from_db_with_filters(conn, table_name, start_date=None, end_date=None):
    query = f"SELECT * FROM {table_name}"
    conditions = []
    if start_date:
        conditions.append(f"DATE(Date) >= '{start_date}'")
    if end_date:
        conditions.append(f"DATE(Date) <= '{end_date}'")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    return pd.DataFrame(result)

# Streamlit App
st.title("AWS RDS Weather Data Viewer")

# 테이블 이름 설정
past_table_name = "past_weather"
future_table_name = "future_weather"

# 사이드바에서 데이터베이스 선택
st.sidebar.title("Database Selection")
db_option = st.sidebar.radio("Choose Database", ("Past Weather", "Future Weather"))

# 데이터베이스 및 테이블 이름 설정
if db_option == "Past Weather":
    table_name = past_table_name
else:
    table_name = future_table_name

# 날짜 필터 입력
st.sidebar.title("Date Filter")
start_date = st.sidebar.date_input("Start Date", datetime.now().date() - timedelta(days=7))
end_date = st.sidebar.date_input("End Date", datetime.now().date())

# Fetch Data 버튼
if st.sidebar.button("Fetch Data"):
    try:
        with get_rds_connection() as conn:
            # 데이터 가져오기
            st.info("Fetching data from AWS RDS...")
            with st.spinner("Loading data..."):
                data = fetch_from_db_with_filters(conn, table_name, start_date, end_date)
                if not data.empty:
                    st.session_state['data'] = data  # 데이터 캐시 저장
                    st.success(f"Data fetched successfully! [total {len(data)} rows]")
                else:
                    st.warning("No data found for the selected filters.")
    except Exception as e:
        st.error(f"An error occurred while connecting to AWS RDS: {e}")

# 캐시된 데이터가 있는 경우에만 추가 필터링 및 조회 가능
if 'data' in st.session_state and not st.session_state['data'].empty:
    st.subheader("Filter Data")
    data = st.session_state['data']

    # Filter by State
    st.sidebar.title("Filter by State")
    try:
        regions = data['Region'].dropna().unique().tolist()
        regions = sorted(filter(None, regions))  # None 값을 제거하고 정렬
        selected_states = st.sidebar.multiselect("Select States", options=regions)
    except KeyError:
        st.warning("No regions available for filtering.")
        selected_states = []

    # Additional Filters
    st.sidebar.title("Additional Filters")
    search_keyword = st.sidebar.text_input("Search by Station Name", "")

    filtered_data = data

    # State Filter 적용
    if selected_states:
        filtered_data = data[data['Region'].isin(selected_states)]

    # Station Name 검색 적용
    if search_keyword:
        filtered_data = filtered_data[filtered_data['Station_Name'].str.contains(search_keyword, case=False, na=False)]

    # 필터링 결과 표시
    if not filtered_data.empty:
        st.subheader("Filtered Data")
        st.dataframe(filtered_data)

        # 데이터 요약
        st.subheader("Data Summary")
        numeric_columns = ['tavg', 'tmin', 'tmax', 'prcp', 'snow', 'avg_wdir', 'pres', 'tsun', 'avg_rhum', 'avg_dwpt']
        summary = filtered_data[numeric_columns].describe().transpose()
        st.write(summary)

        # 데이터 다운로드
        st.subheader("Download Data")
        csv = filtered_data.to_csv(index=False)
        st.download_button(label="Download as CSV", data=csv, file_name="filtered_weather_data.csv", mime="text/csv")

        # 데이터 시각화
        st.subheader("Data Visualization")
        st.write("Select columns to visualize")

        x_col = st.selectbox("X-axis column", options=numeric_columns, index=0, key="x_axis_col")
        y_col = st.selectbox("Y-axis column", options=numeric_columns, index=1, key="y_axis_col")

        if x_col and y_col:
            fig, ax = plt.subplots(figsize=(8, 4))
            sns.scatterplot(data=filtered_data, x=x_col, y=y_col, ax=ax)
            ax.set_title(f"{x_col} vs {y_col}")
            st.pyplot(fig, clear_figure=True)
    else:
        st.warning("No data matches the selected filters.")

