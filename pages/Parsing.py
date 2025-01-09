import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from meteostat import Stations,  Hourly
import logging
import os
import time
import warnings
from dotenv import load_dotenv
import pymysql

# 환경변수 로드
load_dotenv()

# AWS RDS DB 연결 정보 (환경변수)
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

warnings.filterwarnings("ignore", category=DeprecationWarning)

# 로그 디렉토리 설정
if not os.path.exists("logs"):
    os.makedirs("logs")

log_time = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = f"logs/weather_parsing_{log_time}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# DB 연결 함수
def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )

# DB 데이터 초기화 함수
def initialize_db(conn, table_name):
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                Station_ID TEXT,
                Station_Name TEXT,
                Country TEXT,
                Region TEXT,
                WMO TEXT,
                ICAO TEXT,
                Latitude REAL,
                Longitude REAL,
                Elevation REAL,
                Timezone TEXT,
                Date DATE,  -- 날짜 형식을 DATE로 수정
                tavg REAL,
                tmin REAL,
                tmax REAL,
                prcp REAL,
                snow REAL,
                avg_wdir REAL,
                wspd REAL,
                pres REAL,
                tsun REAL,
                avg_rhum REAL,
                avg_dwpt REAL,            
                PRIMARY KEY (Station_ID, Date)
            )
            """)
        conn.commit()  # 변경 사항을 커밋
    except pymysql.MySQLError as e:
        logging.warning(
            f"Error initializing table `{table_name}`: {e}")


# DB 데이터 조회 함수 (날짜 필터 추가)
def fetch_from_db_with_date(conn, table_name, start_date=None, end_date=None):
    data = {'datetime_column': [start_date, end_date]}
    df_date = pd.DataFrame(data)

    # datetime64[ns] 데이터로 변환
    df_date['datetime_column'] = pd.to_datetime(df_date['datetime_column'])

    # .dt.date로 날짜 정보만 추출
    df_date['date_column'] = df_date['datetime_column'].dt.date

    start_date = df_date['date_column'][0]
    end_date = df_date['date_column'][1]

    with conn:
        query = f"SELECT * FROM {table_name}"
        if start_date and end_date:
            query += f" WHERE DATE(Date) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            query += f" WHERE DATE(Date) >= '{start_date}'"
        elif end_date:
            query += f" WHERE DATE(Date) <= '{end_date}'"
        return pd.read_sql(query, conn)


# 과거 데이터 삽입 (중복 데이터 무시)
def insert_past_data(conn, table_name, data):
    inserted_count = 0

    if 'Station ID' in data:
        data.rename(columns={'Station ID': 'Station_ID'}, inplace=True)
    if 'Station Name' in data:
        data.rename(columns={'Station Name': 'Station_Name'}, inplace=True)

    with conn.cursor() as cur:
        for _, row in data.iterrows():
            try:
                row_dict = row.to_dict()

                query = f"""
                    INSERT INTO {table_name} (
                        Station_ID, Station_Name, Country, Region, WMO, ICAO, Latitude, Longitude, Elevation,
                        Timezone, Date, tavg, tmin, tmax, prcp, snow, avg_wdir, wspd, pres, tsun, avg_rhum, avg_dwpt
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        tavg=VALUES(tavg),
                        tmin=VALUES(tmin),
                        tmax=VALUES(tmax),
                        prcp=VALUES(prcp),
                        snow=VALUES(snow),
                        avg_wdir=VALUES(avg_wdir),
                        wspd=VALUES(wspd),
                        pres=VALUES(pres),
                        tsun=VALUES(tsun),
                        avg_rhum=VALUES(avg_rhum),
                        avg_dwpt=VALUES(avg_dwpt);
                """

                cur.execute(query, (
                    row_dict.get('Station_ID'), row_dict.get('Station_Name'), row_dict.get('Country'),
                    row_dict.get('Region'), row_dict.get('WMO'), row_dict.get('ICAO'),
                    row_dict.get('Latitude'), row_dict.get('Longitude'), row_dict.get('Elevation'),
                    row_dict.get('Timezone'), row_dict.get('Date'), row_dict.get('tavg'),
                    row_dict.get('tmin'), row_dict.get('tmax'), row_dict.get('prcp'),
                    row_dict.get('snow'), row_dict.get('avg_wdir'), row_dict.get('wspd'),
                    row_dict.get('pres'), row_dict.get('tsun'), row_dict.get('avg_rhum'), row_dict.get('avg_dwpt')
                ))

                inserted_count += 1

            except Exception as e:
                logging.warning(
                    f"Failed to insert past data for Station {row_dict.get('Station_ID')} at {row_dict.get('Time')}: {e}")

    conn.commit()
    return inserted_count

# 미래 데이터 삽입 또는 업데이트
def upsert_future_data(conn, table_name, data):
    inserted_count = 0
    updated_count = 0

    if 'Station ID' in data:
        data.rename(columns={'Station ID': 'Station_ID'}, inplace=True)
    if 'Station Name' in data:
        data.rename(columns={'Station Name': 'Station_Name'}, inplace=True)

    with conn.cursor() as cur:
        for _, row in data.iterrows():
            try:
                query = f"""
                    INSERT INTO {table_name} (
                        Station_ID, Station_Name, Country, Region, WMO, ICAO, Latitude, Longitude, Elevation,
                        Timezone, Date, tavg, tmin, tmax, prcp, snow, avg_wdir, wspd, pres, tsun, avg_rhum, avg_dwpt
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        tavg=VALUES(tavg),
                        tmin=VALUES(tmin),
                        tmax=VALUES(tmax),
                        prcp=VALUES(prcp),
                        snow=VALUES(snow),
                        avg_wdir=VALUES(avg_wdir),
                        wspd=VALUES(wspd),
                        pres=VALUES(pres),
                        tsun=VALUES(tsun),
                        avg_rhum=VALUES(avg_rhum),
                        avg_dwpt=VALUES(avg_dwpt);
                """
                cur.execute(query, (
                    row['Station_ID'], row['Station_Name'], row['Country'], row['Region'], row['WMO'], row['ICAO'],
                    row['Latitude'], row['Longitude'], row['Elevation'], row['Timezone'], row['Date'], row['tavg'],
                    row['tmin'], row['tmax'], row['prcp'], row['snow'], row['avg_wdir'], row['wspd'],
                    row['pres'], row['tsun'], row['avg_rhum'], row['avg_dwpt']
                ))
                if cur.rowcount == 1:  # 새로 삽입된 경우
                    inserted_count += 1
                else:  # 업데이트된 경우
                    updated_count += 1
            except Exception as e:
                logging.warning(f"Failed to upsert future data for Station {row['Station_ID']} at {row['Date']}: {e}")

    conn.commit()
    return inserted_count, updated_count


# 데이터 삽입 함수
def insert_data_from_csv(conn, table_name, data):
    inserted_count = 0
    updated_count = 0

    with conn.cursor() as cur:
        for _, row in data.iterrows():
            try:
                query = f"""
                    INSERT INTO {table_name} (
                        Station_ID, Station_Name, Country, Region, WMO, ICAO, Latitude, Longitude, Elevation,
                        Timezone, Date, tavg, tmin, tmax, prcp, snow, avg_wdir, wspd, pres, tsun, avg_rhum, avg_dwpt
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        tavg=VALUES(tavg),
                        tmin=VALUES(tmin),
                        tmax=VALUES(tmax),
                        prcp=VALUES(prcp),
                        snow=VALUES(snow),
                        avg_wdir=VALUES(avg_wdir),
                        wspd=VALUES(wspd),
                        pres=VALUES(pres),
                        tsun=VALUES(tsun),
                        avg_rhum=VALUES(avg_rhum),
                        avg_dwpt=VALUES(avg_dwpt);
                """
                cur.execute(query, tuple(row[col] for col in [
                    'Station_ID', 'Station_Name', 'Country', 'Region', 'WMO', 'ICAO',
                    'Latitude', 'Longitude', 'Elevation', 'Timezone', 'Date', 'tavg',
                    'tmin', 'tmax', 'prcp', 'snow', 'avg_wdir', 'wspd',
                    'pres', 'tsun', 'avg_rhum', 'avg_dwpt'
                ]))
                if cur.rowcount == 1:
                    inserted_count += 1
                else:
                    updated_count += 1
            except Exception as e:
                logging.warning(f"Failed to process data for Station {row['Station_ID']} at {row['Date']}: {e}")

    conn.commit()
    return inserted_count, updated_count



# 날짜별 평균 바람 방향 계산 함수
def calculate_wind_direction(df):
    # 날짜 열 추가
    df['Date'] = df['Time'].dt.date

    # 라디안으로 변환
    df['WindDirection_rad'] = np.deg2rad(df['wdir'])

    # X, Y 성분 계산
    df['x'] = np.cos(df['WindDirection_rad'])
    df['y'] = np.sin(df['WindDirection_rad'])

    # X, Y 성분 평균
    avg_x = df['x'].mean()
    avg_y = df['y'].mean()

    # 평균 방향 계산 (라디안)
    avg_direction_rad = np.arctan2(avg_y, avg_x)

    # 라디안을 도(degree)로 변환하고 0~360도로 조정
    avg_direction_deg = np.degrees(avg_direction_rad)
    if avg_direction_deg < 0:
        avg_direction_deg += 360

    return avg_direction_deg

# 데이터 파싱 함수
def parse_weather_data(start_date, end_date):
    stations = Stations().region('US').fetch()
    all_weather_data = pd.DataFrame()
    failed_stations = []

    progress_bar = st.progress(0)  # 프로그레스 바 초기화
    total_stations = len(stations)
    success_count = 0
    failure_count = 0

    # 텍스트 출력 영역 초기화
    metrics_placeholder = st.empty()

    # 기간 출력
    st.write(f"**Parsing period:** {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    start_time = time.time()  # 파싱 시작 시간 기록

    for index, station in enumerate(stations.itertuples(), start=1):
        try:
            station_id = station.Index
            data = Hourly(station_id, start_date, end_date).fetch()
            # print(station_id, data)

            if not data.empty:
                # 메타데이터 추가
                data['Station ID'] = station_id
                data['Station Name'] = station.name
                data['Country'] = station.country
                data['Region'] = station.region
                data['WMO'] = station.wmo
                data['ICAO'] = station.icao
                data['Latitude'] = station.latitude
                data['Longitude'] = station.longitude
                data['Elevation'] = station.elevation
                data['Timezone'] = station.timezone
                data['Time'] = data.index

                # 날짜만 추출하여 'Date' 열 생성
                data['Date'] = data['Time'].dt.date

                # Daily 데이터로 집계
                daily_data = data.groupby(['Station ID', 'Date']).agg({
                    'temp': ['mean', 'min', 'max'],  # 평균, 최저, 최고 온도
                    'prcp': 'sum',  # 강수량 합계
                    'snow': 'sum',  # 적설량 합계
                    # 'wdir': 'mean',  # 평균 풍향
                    'wspd': 'mean',  # 평균 풍속
                    'pres': 'mean',  # 평균 기압
                    'tsun': 'sum',  # 일조 시간 합계
                    'rhum': 'mean',  # 평균 상대습도
                    'dwpt': 'mean'   # 평균 이슬점
                }).reset_index()

                # 열 이름 정리
                daily_data.columns = ['Station ID', 'Date', 'tavg', 'tmin', 'tmax', 'prcp', 'snow',
                                      'wspd', 'pres', 'tsun', 'avg_rhum', 'avg_dwpt']

                wind_directions = data.groupby(['Station ID', 'Date']).apply(calculate_wind_direction).reset_index(name='avg_wdir')
                daily_data = pd.merge(daily_data, wind_directions, on=['Station ID', 'Date'])

                # 메타데이터 추가
                meta_columns = ['Station Name', 'Country', 'Region', 'WMO', 'ICAO', 'Latitude', 'Longitude', 'Elevation', 'Timezone']

                # 메타데이터 열을 고유값으로 채워서 daily_data에 추가
                for col in meta_columns:
                    # 'Station ID'와 'Date'를 기준으로 고유값을 가져와 병합
                    metadata = data[['Station ID', col]].drop_duplicates()
                    daily_data = pd.merge(daily_data, metadata, on='Station ID', how='left')

                if 'Station ID' in daily_data:
                    daily_data.rename(columns={'Station ID': 'Station_ID'}, inplace=True)
                if 'Station Name' in daily_data:
                    daily_data.rename(columns={'Station Name': 'Station_Name'}, inplace=True)

                # 열 순서 정렬
                daily_data = daily_data[[
                    'Station_ID', 'Station_Name', 'Country', 'Region', 'WMO', 'ICAO', 'Latitude', 'Longitude', 'Elevation',
                    'Timezone', 'Date', 'tavg', 'tmin', 'tmax', 'prcp', 'snow', 'avg_wdir', 'wspd', 'pres', 'tsun', 'avg_rhum', 'avg_dwpt'
                ]]

                # 데이터 추가
                all_weather_data = pd.concat([all_weather_data, daily_data])
                success_count += 1
            else:
                raise ValueError("No data fetched")
        except Exception as e:
            # 파싱 실패 시 기록
            failed_stations.append((station_id, station.name, str(e)))
            failure_count += 1
            logging.warning(f"Failed to parse station {station.name} (ID: {station_id}): {e}")

        # 진행 상황 업데이트
        progress_bar.progress(index / total_stations)
        elapsed_time = time.time() - start_time
        remaining_time = (elapsed_time / index) * (total_stations - index)
        completion_time = datetime.now() + timedelta(seconds=remaining_time)

        # 텍스트 영역 업데이트
        metrics_placeholder.markdown(f"""
        **Stations Processed**: {index}/{total_stations}  
        **Success Count**: {success_count}  
        **Failure Count**: {failure_count}  
        **Time Remaining**: {int(remaining_time // 60)}m {int(remaining_time % 60)}s  
        **Estimated Completion**: {completion_time.strftime("%H:%M:%S")}
        """)

    # 실패 스테이션 로그 저장
    if failed_stations:
        failed_df = pd.DataFrame(failed_stations, columns=['Station ID', 'Station Name', 'Error'])
        failed_csv = f"logs/failed_stations_{log_time}.csv"
        failed_df.to_csv(failed_csv, index=False)
        st.warning(f"Failed station details saved to `{failed_csv}`")

    return all_weather_data, failed_stations


# Streamlit App
st.title("Weather Data Parsing")

# DB 초기화
past_table_name = "past_weather"
future_table_name = "future_weather"

db_conn = get_db_connection()

initialize_db(db_conn, past_table_name)
initialize_db(db_conn, future_table_name)

# 데이터 파싱
st.sidebar.title("Weather Data Parsing")
parse_past = st.sidebar.button("Parse Past Week's Real Data")
parse_future = st.sidebar.button("Parse Next Week's Forecast Data")

# 오늘 날짜 기준으로 지난 1주일, 미래 1주일 계산
today = datetime.now()
today = datetime(today.year, today.month, today.day)

st.sidebar.title("Date Filter")

past_start_date = st.sidebar.date_input("Past Start Date", today - timedelta(days=7))
past_start_date_time = datetime.combine(past_start_date, datetime.min.time())

past_end_date = st.sidebar.date_input("Past End Date or Future Start Date", datetime.now().date())
past_end_date_time = datetime.combine(past_end_date, datetime.min.time())

future_end_date = st.sidebar.date_input("Future End Date", datetime.now().date() + timedelta(days=7))
future_end_date_time = datetime.combine(future_end_date, datetime.min.time())

if parse_past:
    st.info("Parsing past week's weather data...")
    with st.spinner("Processing past weather data..."):
        past_weather_data, failed_stations = parse_weather_data(past_start_date_time, past_end_date_time)
        inserted_count = insert_past_data(db_conn, past_table_name, past_weather_data)
    st.success(f"Past week's weather data parsed. Inserted rows: {inserted_count}")

    if failed_stations:
        st.warning(f"{len(failed_stations)} stations failed to parse:")
        failed_df = pd.DataFrame(failed_stations, columns=['Station ID', 'Station Name', 'Error'])
        st.write(failed_df)

if parse_future:
    st.info("Parsing next week's weather forecast data...")
    with st.spinner("Processing future weather data..."):
        future_weather_data, failed_stations = parse_weather_data(past_end_date_time, future_end_date_time)
        inserted_count, updated_count = upsert_future_data(db_conn, future_table_name, future_weather_data)
    st.success(f"Future week's weather data parsed. Inserted rows: {inserted_count}, Updated rows: {updated_count}")

    if failed_stations:
        st.warning(f"{len(failed_stations)} stations failed to parse:")
        failed_df = pd.DataFrame(failed_stations, columns=['Station ID', 'Station Name', 'Error'])
        st.write(failed_df)
