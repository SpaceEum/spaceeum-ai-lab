import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import pyupbit  # Added to fetch current BTC price

# 데이터베이스 연결 함수
def get_connection():
    return sqlite3.connect('xrp_trades.db') # autotrade.py와 동일한 DB 사용

# 데이터 로드 함수
def load_data():
    conn = get_connection()
    query = "SELECT * FROM trades"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# 초기 투자 금액 계산 함수
def calculate_initial_investment(df):
    initial_krw_balance = df.iloc[0]['krw_balance']
    initial_xrp_balance = df.iloc[0]['xrp_balance'] # 컬럼명 수정
    initial_xrp_price = df.iloc[0]['xrp_krw_price'] # 컬럼명 수정
    initial_total_investment = initial_krw_balance + (initial_xrp_balance * initial_xrp_price)
    return initial_total_investment

# 현재 투자 금액 계산 함수
def calculate_current_investment(df):
    current_krw_balance = df.iloc[-1]['krw_balance']
    current_xrp_balance = df.iloc[-1]['xrp_balance'] # 컬럼명 수정
    current_xrp_price = pyupbit.get_current_price("KRW-XRP")
    current_total_investment = current_krw_balance + (current_xrp_balance * current_xrp_price)
    return current_total_investment

# 메인 함수
def main():
    st.title('XRP Trades Viewer')

    # 데이터 로드
    df = load_data()

    if df.empty:
        st.warning('No trade data available.')
        return

    # 초기 투자 금액 계산
    initial_investment = calculate_initial_investment(df)

    # 현재 투자 금액 계산
    current_investment = calculate_current_investment(df)

    # 수익률 계산
    profit_rate = ((current_investment - initial_investment) / initial_investment) * 100

    # 수익률 표시
    st.header(f'📈 Current Profit Rate: {profit_rate:.2f}%')

    # 기본 통계
    st.header('Basic Statistics')
    st.write(f"Total number of trades: {len(df)}")
    st.write(f"First trade date: {df['timestamp'].min()}")
    st.write(f"Last trade date: {df['timestamp'].max()}")

    # 거래 내역 표시
    st.header('Trade History')
    # Sort by timestamp in descending order to show recent trades first
    df_sorted = df.sort_values(by='timestamp', ascending=False)
    st.dataframe(df_sorted)

    # 거래 결정 분포
    st.header('Trade Decision Distribution')
    decision_counts = df['decision'].value_counts()
    if not decision_counts.empty:
        fig = px.pie(values=decision_counts.values, names=decision_counts.index, title='Trade Decisions')
        st.plotly_chart(fig)
    else:
        st.write("No trade decisions to display.")

    # XRP 잔액 변화
    st.header('XRP Balance Over Time')
    fig = px.line(df, x='timestamp', y='xrp_balance', title='XRP Balance') # 컬럼명 수정
    st.plotly_chart(fig)

    # KRW 잔액 변화
    st.header('KRW Balance Over Time')
    fig = px.line(df, x='timestamp', y='krw_balance', title='KRW Balance')
    st.plotly_chart(fig)

    # XRP 평균 매수가 변화
    st.header('XRP Average Buy Price Over Time')
    fig = px.line(df, x='timestamp', y='xrp_avg_buy_price', title='XRP Average Buy Price') # 컬럼명 수정
    st.plotly_chart(fig)

    # XRP 가격 변화
    st.header('XRP Price Over Time') # Changed header
    fig = px.line(df, x='timestamp', y='xrp_krw_price', title='XRP Price (KRW)') # 컬럼명 수정
    st.plotly_chart(fig)

if __name__ == "__main__":
    main()