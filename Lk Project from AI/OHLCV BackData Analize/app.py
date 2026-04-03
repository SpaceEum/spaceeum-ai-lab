import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from src.data_loader import DataLoader
from src.feature_engineer import FeatureEngineer
from src.model_trainer import ModelTrainer
from src.predictor import Predictor

st.set_page_config(page_title="Antigravity: OHLCV Predictor", layout="wide")

st.title("🚀 Project Antigravity: Daily OHLCV Predictor")
st.markdown("머신러닝 기반 T+1 종가 예측 대시보드 (LightGBM MVP)")

def get_instances():
    return DataLoader(), FeatureEngineer(), ModelTrainer(), Predictor()

dl, fe, mt, pr = get_instances()

# Sidebar controls
import os

st.sidebar.header("Settings")
available_symbols = dl.get_available_symbols()

if not available_symbols:
    st.error("데이터 폴더에 CSV 파일이 없습니다.")
    st.stop()

# Detect trained symbols by checking ./models directory
trained_symbols = set()
if os.path.exists("./models"):
    for file in os.listdir("./models"):
        if file.endswith("_lgb.pkl"):
            trained_symbols.add(file.replace("_lgb.pkl", ""))
            
if trained_symbols:
    sorted_trained = sorted(list(trained_symbols))
    st.sidebar.markdown("✅ **학습 완료된 종목 목록**")
    
    # Use session state to manage the selected symbol between the trained list and the main dropdown
    if 'selected_symbol' not in st.session_state:
        st.session_state.selected_symbol = available_symbols[0] if available_symbols else None

    # Create a compact radio button list for trained symbols
    selected_trained = st.sidebar.radio(
        "여기서 선택하세요 👇",
        options=["(선택안함)"] + sorted_trained,
        index=0,
        help="학습 완료된 종목 중 하나를 빠르게 선택할 수 있습니다."
    )
    
    if selected_trained != "(선택안함)":
        st.session_state.selected_symbol = selected_trained
else:
    st.sidebar.warning("⚠️ 학습된 종목이 아직 없습니다. 전체 재학습을 먼저 진행하세요.")

st.sidebar.markdown("---")

# Use the session_state value for the selectbox index
default_idx = available_symbols.index(st.session_state.selected_symbol) if st.session_state.selected_symbol in available_symbols else 0

selected_symbol = st.sidebar.selectbox(
        "🔤 모든 종목 검색 및 선택",
        options=available_symbols,
        index=default_idx,
        help="모든 종목을 검색하여 선택할 수 있습니다."
    )

# Update session state if the user manually changes the selectbox
if selected_symbol != st.session_state.selected_symbol:
    st.session_state.selected_symbol = selected_symbol

action = st.sidebar.radio("작업 선택", ["대시보드 보기 (예측)", "이어서 학습 (Incremental)", "전체 재학습 (Full Train)"])

# Load Data
symbol_data = dl.load_symbol_data(selected_symbol)
df_1d = symbol_data.get('1D', pd.DataFrame())

if df_1d.empty:
    st.error(f"{selected_symbol}의 1D 데이터가 없습니다.")
    st.stop()

# Process Features
with st.spinner('피처 엔지니어링 수행 중...'):
    processed_df = fe.process_symbol(symbol_data)

if action == "전체 재학습 (Full Train)":
    st.subheader(f"🛠️ {selected_symbol} 전체 모델 재학습")
    st.info("과거 데이터를 전부 사용하여 모델을 처음부터 다시 학습시킵니다. 데이터가 많을 경우 시간이 소요될 수 있습니다.")
    if st.button("학습 시작 (Start Training)"):
        with st.spinner("모델 학습 중..."):
            success = mt.train(processed_df, selected_symbol)
            if success:
                st.success(f"{selected_symbol} 모델 훈련 및 저장 완료!")
            else:
                st.error("학습에 실패했습니다. 데이터가 충분한지 확인하세요.")
                
elif action == "이어서 학습 (Incremental)":
    st.subheader(f"⚡ {selected_symbol} 점진적 이어서 학습")
    st.info("기존 모델에 가장 최근 N일의 데이터만 반영하여 빠르고 가볍게 모델을 트렌드에 맞게 업데이트합니다.")
    recent_days = st.number_input("학습할 최근 데이터 일수 (N days)", min_value=5, max_value=365, value=30)
    
    if st.button("점진적 학습 시작 (Start Incremental Update)"):
        with st.spinner(f"최근 {recent_days}일 데이터로 모델 업데이트 중..."):
            success = mt.train_incremental(processed_df, selected_symbol, recent_n_days=recent_days)
            if success:
                st.success(f"{selected_symbol} 모델 업데이트 완료! (최신 트렌드 반영됨)")
            else:
                st.error("점진적 학습 실패. 전체 재학습을 먼저 진행했는지 확인하세요.")
                
elif action == "대시보드 보기 (예측)":
    # Inference
    prediction = pr.predict_next_day(processed_df, selected_symbol)
    
    if "error" in prediction:
        st.warning("⚠️ 학습된 모델이 없습니다. 좌측 메뉴에서 '모델 재학습'을 먼저 실행해주세요.")
    else:
        # Metics row
        col1, col2, col3 = st.columns(3)
        col1.metric("최근 종가 (Current Close)", f"{prediction['current_close']:.4f} USDT")
        col2.metric("내일 예상 종가 (Predicted Close)", f"{prediction['predicted_close']:.4f} USDT", f"{prediction['predicted_return_pct']:.2f}%")
        
        # We need historical predictions to calculate MAE/RMSE and draw a chart
        # For this MVP dashboard, we will simulate historical predictions using the model
        st.subheader("실제 가격 vs 모델 예측 추이 (최근 100일)")
        
        # To show the chart, we need the last 100 days of features, predict on them, and compare
        try:
            import joblib
            import os
            model = joblib.load(os.path.join("./models", f"{selected_symbol}_lgb.pkl"))
            scaler = joblib.load(os.path.join("./models", f"{selected_symbol}_scaler.pkl"))
            
            tail_df = processed_df.tail(100).copy()
            X_eval, y_eval = mt.prepare_data(tail_df)
            X_eval_scaled = scaler.transform(X_eval)
            pred_log_ret = model.predict(X_eval_scaled)
            
            # Reconstruct prices from returns
            # target return is ln(Close_t+1 / Close_t).
            # So Predicted Close_t+1 = Close_t * exp(predicted_return)
            tail_df['Predicted_Return'] = pred_log_ret
            tail_df['Predicted_Close'] = tail_df['Close'] * np.exp(tail_df['Predicted_Return'])
            # Shift back so Predicted_Close aligns with the actual T+1 date target
            # Current row has Close_t. The target it predicts is for t+1.
            # So the Predicted_Close calculated is actually what we expect FOR TOMORROW relative to the row's date.
            
            # Simple line chart
            fig = go.Figure()
            # We want to plot the actual Target (which is Close_t+1) vs Predicted_Close
            actual_next_close = tail_df['Close'].shift(-1)
            
            fig.add_trace(go.Scatter(x=tail_df['Open_Time'], y=actual_next_close, mode='lines', name='실제 내일 종가(Actual)', line=dict(color='blue')))
            fig.add_trace(go.Scatter(x=tail_df['Open_Time'], y=tail_df['Predicted_Close'], mode='lines', name='예측 내일 종가(Predicted)', line=dict(color='orange', dash='dot')))
            
            fig.update_layout(title="Predicted vs Actual", xaxis_title="Date", yaxis_title="Price")
            st.plotly_chart(fig, use_container_width=True)
            
            # Error Metrics
            mae = np.mean(np.abs(tail_df['Predicted_Close'][:-1] - actual_next_close[:-1]))
            mape = np.mean(np.abs((actual_next_close[:-1] - tail_df['Predicted_Close'][:-1]) / actual_next_close[:-1])) * 100
            
            sc1, sc2 = st.columns(2)
            sc1.metric("평균 절대 오차 (MAE)", f"{mae:.4f}")
            sc2.metric("평균 절대 오차율 (MAPE)", f"{mape:.2f}%")
            
        except Exception as e:
            st.error(f"차트 렌더링 중 오류 발생: {e}")
