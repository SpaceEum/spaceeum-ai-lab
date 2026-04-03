# 🚀 Project Antigravity: Multi-Timeframe Price Prediction

이 프로젝트는 바이낸스 선물(Futures) USDT 마켓의 데이터를 기반으로, 월봉(1M)과 주봉(1W)의 큰 흐름 속에서 일봉(1D)의 변동성을 학습하여 내일의 OHLCV를 예측하는 머신러닝 시스템입니다.

## 1. 프로젝트 개요
- **프로젝트명**: antigravity (중력을 거스르는 수익률 지향)
- **목표**: T+1 시점의 일봉(1D) OHLCV 예측
- **데이터 소스**: 로컬 저장된 타임프레임별 OHLCV (1M, 1W, 1D)
- **핵심 로직**: 상위 타임프레임(1M, 1W)의 추세 지표를 하위 타임프레임(1D)의 피처로 주입하여 시계열 계층 구조 학습

## 2. 데이터 아키텍처 및 로드 전략

### 2.1 데이터 경로 구조
데이터는 사용자 지정 경로에서 자동으로 스캔하도록 설정합니다.
- **Root**: `G:\내 드라이브\LK Project\Lk Project from AI\OHLCV`
- **Sub-folders**:
  - `Binance_Futures_USDT_1M_ohlcv/`
  - `Binance_Futures_USDT_1W_ohlcv/`
  - `Binance_Futures_USDT_1D_ohlcv/`

### 2.2 데이터 병합 (Data Alignment)
1D 데이터를 기준으로, 해당 날짜가 속한 주(Week)와 월(Month)의 데이터를 매핑합니다.
- **Key**: `Open_Time` (Timestamp)
- **Logic**: 1D 레코드마다 현재 진행 중인 1W, 1M의 OHLCV 정보와 보조지표를 Feature로 결합합니다.
- **🚨 Data Leakage 방지 전략**: 미래 참조 오류를 막기 위해 다중 타임프레임 병합 시 반드시 **Rolling Window(이동 누적) 방식**을 사용하거나, 전일 기준으로 **'확정된 과거 타임프레임 데이터'**만을 피처로 취합합니다.

## 3. 피처 엔지니어링 (Feature Engineering)
각 타임프레임별로 기술적 지표를 생성하여 모델에 시장 심리를 전달합니다.
- **추세 지표**: 이동평균선(SMA/EMA 20, 50, 200), MACD (1M, 1W의 추세 방향성 파악)
- **모멘텀 지표**: RSI, Stochastic (과매수/과매도 구간 파악)
- **변동성 지표**: Bollinger Bands, ATR (일봉의 변동 폭 예측을 위한 필수 지표)
- **거래량 지표**: OBV, Volume MA (매수/매수세 강도)
- **시간 피처**: 요일(Day of week), 월(Month of year) 등의 주기성 데이터
- **🔥 파생상품 특화 피처 (Crypto Futures Features)**:
  - **Open Interest (미결제약정)**: 시장 참여 자금 추세를 통한 모멘텀 파악
  - **Funding Rate (펀딩비)**: 롱/숏 쏠림 현상을 활용한 역추세 시그널 탐지
  - **Liquidation Data (청산 데이터)**: 대규모 청산 맵 기반의 급격한 꼬리(Tail) 변동성 포착
- **🌍 거시경제 및 시장 환경 피처 (Macro & Regime Features)**:
  - **Bitcoin Dominance (비트코인 도미넌스)**: 알트코인/비트코인 자금 순환 사이클 파악
  - **Macro Indicators**: S&P500 등락률, DXY(달러 인덱스) 변동 등 글로벌 유동성 지표 (선택적 병합)
- **🏛️ 워런 버핏(Value Investing) 철학 기반 내재가치 피처 (Fundamental & Intrinsic Proxies)**:
  - *"가격은 우리가 내는 것이고, 가치는 우리가 얻는 것이다."*라는 철학을 암호화폐 시장에 맞게 변형하여 적용합니다.
  - **Network Value to Transactions (NVT) Ratio**: 코인의 시가총액을 블록체인 네트워크 상의 전송량으로 나눈 값. (주식의 PER과 유사하게 고평가/저평가 판단)
  - **Active Addresses & Tx Count**: 주식의 '매출액/활성 고객수'에 해당하는 실질적 네트워크 채택률.
  - **Mean Reversion (평균 회귀)**: 가격이 내재가치(장기 이동평균선 중심)와 과도하게 멀어졌을 때 펀더멘털로 회귀하려는 성질을 피처로 수치화 (Margin of Safety 확보 목적).




## 4. 머신러닝 모델링 전략

### 4.1 모델 아키텍처 후보
개별 단일 모델의 한계를 극복하기 위해 분류-회귀 2단계 앙상블(Ensemble) 구조를 도입합니다.
- **1단계 (방향성 분류 - Classification)**: 내일 가격의 상승/하락 확률을 먼저 이진 분류. (예: XGBoost / LightGBM)
- **2단계 (변동성 예측 - Regression)**: 예측된 방향에 맞추어 변동성(High-Low 진폭)을 정밀 예측. (예: LSTM / TFT)


### 4.2 학습 방식 (Hierarchical Learning)
- **Input (3D Tensor)**: (Batch_Size, Sequence_Length, Features) 형태로 $[1D_{Features_t}, 1W_{Features_t}, 1M_{Features_t}]$ 병합 데이터를 입력.
- **Sequence Padding & Masking**: 상위 타임프레임(1W, 1M) 정보가 일봉(1D) 주기에 맞춰 동일하게 늘어나는(Forward Fill) 현상으로 인한 시퀀스 중복(Redundancy)을 방지하기 위한 어텐션 마스킹(Attention Masking) 기법 적용.
- **Target**: 과적합 방지 및 시계열 비정상성 해결을 위해 절대 가격($[OHLCV]$) 대신 **로그 수익률(Log Returns)**이나 **전일 대비 범주형 증감률(Percentage Change)**을 예측.
- **Window Size**: 과거 $N$일간의 데이터를 시퀀스로 입력하여 내일의 변동률을 도출.


### 4.3 손실 함수 고도화 및 불균형 처리 (Loss Function & Imbalance Handling)
단순 값 오차 최소화가 아닌 실제 트레이딩 수익률에 맞춘 맞춤형 학습을 적용합니다.
- **안전마진(Margin of Safety) 반영 손실 함수**: 워런 버핏의 '제1원칙: 돈을 잃지 마라'를 수식화하여, 예측가 하단 밴드(하락 리스크)를 돌파할 확률이 높을 때 강한 페널티 부과.
- **클래스 불균형(Class Imbalance) 대응**: 횡보장이 길어지거나 상승/하락 비율이 맞지 않는 금융 데이터 특성을 고려해, **Focal Loss**를 적용하거나 **Sample Weights(샘플 가중치)**를 조절하여 소수 클래스(급등락 구간)의 예측력을 높입니다.
- **Directional Accuracy Penalty**: 방향(Up/Down)을 틀리게 예측했을 경우 가중 페널티를 부여합니다.
- **Profit-centric Loss (Sharpe Ratio Loss)**: 예상 시뮬레이션 수익금이나 샤프 지수를 커스텀 손실 함수로 반영하여 '예측 오차 최소화'보다 '누적 수익 극대화'에 모델 최적화의 초점을 맞춥니다.




## 5. 시스템 구현 로드맵

### Phase 1: Data Pipeline (데이터 전처리)
- 종목 리스트 확보.
- 타임프레임별 CSV 로드 및 데이터 무결성 체크(결측치 처리).
- 타임스탬프 기준 1M/1W -> 1D 병합 모듈 개발.

### Phase 2: Feature Engineering (지표 생성)
- `pandas_ta` 또는 `TA-Lib`를 활용한 지표 생성 자동화.
- **데이터 스케일링 (Data Scaling)**: 금융 데이터의 극단치(Outlier)에 강건한 `RobustScaler`를 기본으로 하되, RNN/LSTM 계열은 `MinMaxScaler` 또는 로그 정규화(Log Normalization)를 혼합 적용(Pipeline 구성).
- **다중공선성(Multicollinearity) 검증**: 피처 간 상관계수(Correlation Matrix) 및 VIF(Variance Inflation Factor) 분석을 통한 중복 피처 제거.


### Phase 3: Model Training & Tuning (모델 학습)
- 종목별 학습 또는 섹터별 통합 학습 모델 구축.
- Hyperparameter Optimization (`Optuna` 활용).
- **엄격한 시뮬레이션 검증 (Realistic Walk-forward Validation)**: 
  - 단순 통계적 지표(Accuracy, RMSE)가 아닌, **거래 수수료(Fee)와 슬리피지(Slippage)를 반영한 백테스트 엔진**을 결합하여 과적합을 필터링합니다.

### Phase 4: Prediction & Inference (예측 및 활용)
- 최신 데이터 로드 후 내일의 OHLCV 범위 및 진폭 예측값 출력.
- 예측 결과의 신뢰도(Confidence Score) 산출.
- **실전 트레이딩 제어 로직**: Confidence Score가 낮거나, 예측 기대 수익률이 [거래 수수료+슬리피지]보다 낮을 경우 매매를 보류(Skip)하는 방어 로직 설계.


## 6. 기대 효과 및 주의사항
- **기대 효과**: 상위 프레임의 단단한 추세(매크로)와 네트워크 네트워크 내재가치(Fundamental)를 병합 학습함으로써, 1D 단기 가격 노이즈에 휩쓸리지 않고 '이유 있는 가치 상승'에만 투자하는 안정적인 예측 예측 가능.
- **안전마진 확보**: 무리한 고점 돌파 추격매수를 지양하고, 통계적인 평균 회귀 특성을 살려 리스크를 최소화 (Rule No. 1: Never lose money).
- **주의사항**: 암호화폐 선물 거래 특성상 내재가치가 가격에 반영되는 속도가 늦을 수 있으며, 펀딩비(Funding Rate) 및 변동성 급증 시의 슬리피지 고려 필요.


## 7. 피드백 루프 및 모델 개선 전략 (Feedback Loop & Continuous Learning)
예측값과 실제 데이터를 지속적으로 비교하여 모델의 오차를 줄이고 성능을 고도화합니다.

### 7.1 오차 분석 (Error Analysis)
- **성능 지표**: MAE (Mean Absolute Error), RMSE (Root Mean Squared Error), MAPE (Mean Absolute Percentage Error)를 통해 예측값($\hat{y}$)과 실제값($y$)의 차이를 정밀 추적합니다.
- **방향성 검증**: OHLCV의 단순 수치 오차뿐만 아니라, 추세(Up/Down) 전환점에 대한 예측 성공률을 별도로 관리합니다.

### 7.2 모델 재학습 및 강화 (Model Fine-tuning)
- **온라인 학습 (Incremental Learning)**: 새로운 데이터가 생성될 때마다 모델을 점진적으로 업데이트하여 최신 시장 트렌드를 반영합니다.
- **오차 기반 특징 가중치 조정**: 오차가 크게 발생한 구간의 특성(Feature)을 분석하여 핵심 지표의 가중치를 조정하거나 새로운 피처를 탐색합니다.
- **자동 모델 업데이트 (MLOps Pipeline)**: 
  - 설정된 성능 임계값(Threshold) 이하로 지표가 하락할 경우 (Data/Concept Drift 발생 감지), 전체 데이터셋을 기반으로 하이퍼파라미터 최적화(Optuna)를 포함한 전면 재학습을 트리거(Trigger)합니다.
  - 모델 버전 관리(Model Registry)를 통해 이전 최고 성능 모델로의 자동 롤백(Rollback) 기능 포함.

