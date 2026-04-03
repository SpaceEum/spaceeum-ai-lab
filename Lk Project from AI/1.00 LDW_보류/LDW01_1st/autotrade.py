import os
from dotenv import load_dotenv
import pyupbit
import pandas as pd
import json # AI용 데이터 직렬화에 사용
from openai import OpenAI # type: ignore
import time
import base64
from PIL import Image # type: ignore
import io
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, WebDriverException, NoSuchElementException
import logging
from pydantic import BaseModel # type: ignore
import sqlite3
from datetime import datetime, timedelta
import schedule

# .env 파일에 저장된 환경 변수를 불러오기 (API 키 등)
load_dotenv()

# 로깅 설정 - 로그 레벨을 INFO로 설정하여 중요 정보 출력
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Upbit 객체 생성
access = os.getenv("UPBIT_ACCESS_KEY")
secret = os.getenv("UPBIT_SECRET_KEY")
if not access or not secret:
    logger.error("API keys not found. Please check your .env file.")
    raise ValueError("Missing API keys. Please check your .env file.")
upbit = pyupbit.Upbit(access, secret)

# OpenAI 구조화된 출력 체크용 클래스
class TradingDecision(BaseModel):
    decision: str
    percentage: int
    reason: str

# SQLite 데이터베이스 초기화 함수 - 거래 내역을 저장할 테이블을 생성
def init_db():
    conn = sqlite3.connect('xrp_trades.db') # XRP 데이터베이스 사용
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  timestamp TEXT,
                  decision TEXT,
                  percentage INTEGER,
                  reason TEXT,
                  xrp_balance REAL,
                  krw_balance REAL,
                  xrp_avg_buy_price REAL,
                  xrp_krw_price REAL
                  )''')
    conn.commit()
    return conn

# 거래 기록을 DB에 저장하는 함수
def log_trade(conn, decision, percentage, reason, xrp_balance, krw_balance, xrp_avg_buy_price, xrp_krw_price):
    c = conn.cursor()
    timestamp = datetime.now().isoformat()
    c.execute("""INSERT INTO trades 
                 (timestamp, decision, percentage, reason, xrp_balance, krw_balance, xrp_avg_buy_price, xrp_krw_price)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", # reflection 관련 부분 제거
              (timestamp, decision, percentage, reason, xrp_balance, krw_balance, xrp_avg_buy_price, xrp_krw_price))
    conn.commit()

# 최근 투자 기록 조회
def get_recent_trades(conn, days=7):
    c = conn.cursor()
    seven_days_ago = (datetime.now() - timedelta(days=days)).isoformat()
    c.execute("SELECT * FROM trades WHERE timestamp > ? ORDER BY timestamp DESC", (seven_days_ago,))
    columns = [column[0] for column in c.description]
    return pd.DataFrame.from_records(data=c.fetchall(), columns=columns)

# 20기간 단순이동평균선(SMA) 계산 함수
def calculate_sma(df, window=20):
    """DataFrame과 window 값을 받아 SMA를 계산하여 Series로 반환합니다."""
    if 'close' not in df.columns:
        logger.error("DataFrame에 'close' 컬럼이 없습니다.")
        return None
    if len(df) < window:
        logger.warning(f"데이터 길이({len(df)})가 SMA window({window})보다 짧습니다. SMA를 계산할 수 없습니다.")
        return pd.Series([None] * len(df), index=df.index) # 모든 값을 None으로 채운 Series 반환
    return df['close'].rolling(window=window).mean()

# 일목균형표(Ichimoku Cloud) 계산 함수
def calculate_ichimoku(df, tenkan_period=9, kijun_period=26, senkou_b_period=26):
    """
    DataFrame과 기간을 받아 일목균형표 주요 값들의 최신 값을 담은 딕셔너리를 반환합니다.
    참고: 선행스팬은 미래에 플롯되지만, 여기서는 현재 시점의 계산 값을 제공합니다.
    후행스팬은 현재 종가를 kijun_period 전의 종가와 비교하기 위한 컨텍스트를 제공합니다.
    """
    if not all(col in df.columns for col in ['high', 'low', 'close']):
        logger.error("DataFrame에 'high', 'low', 'close' 컬럼이 모두 존재해야 합니다.")
        return {}

    # Tenkan-sen (전환선)
    tenkan_sen = (df['high'].rolling(window=tenkan_period).max() + df['low'].rolling(window=tenkan_period).min()) / 2
    # Kijun-sen (기준선)
    kijun_sen = (df['high'].rolling(window=kijun_period).max() + df['low'].rolling(window=kijun_period).min()) / 2
    # Senkou Span A (선행스팬 A - 현재 값)
    senkou_span_a = (tenkan_sen + kijun_sen) / 2
    # Senkou Span B (선행스팬 B - 현재 값, kijun_period 사용)
    senkou_span_b = (df['high'].rolling(window=senkou_b_period).max() + df['low'].rolling(window=senkou_b_period).min()) / 2
    
    # Chikou Span (후행스팬) - 현재 종가와 kijun_period 전의 종가
    current_close_for_chikou = df['close'].iloc[-1] if not df.empty else None
    lagged_close_for_chikou = df['close'].iloc[-kijun_period] if len(df) >= kijun_period else None

    return {
        'tenkan_sen': tenkan_sen.iloc[-1] if not tenkan_sen.empty else None,
        'kijun_sen': kijun_sen.iloc[-1] if not kijun_sen.empty else None,
        'senkou_span_a_current': senkou_span_a.iloc[-1] if not senkou_span_a.empty else None,
        'senkou_span_b_current': senkou_span_b.iloc[-1] if not senkou_span_b.empty else None,
        'chikou_span_current_price': current_close_for_chikou,
        'chikou_span_lagged_price': lagged_close_for_chikou
    }

### Selenium 관련 함수
def create_driver():
    env = os.getenv("ENVIRONMENT")
    logger.info("ChromeDriver 설정 중...")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    try:
        if env == "local":
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
        elif env == "ec2":
            service = Service('/usr/bin/chromedriver')
        else:
            raise ValueError(f"Unsupported environment. Only local or ec2: {env}")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
    except Exception as e:
        logger.error(f"ChromeDriver 생성 중 오류 발생: {e}")
        raise

# XPath로 Element 찾기
def click_element_by_xpath(driver, xpath, element_name, wait_time=10):
    try:
        element = WebDriverWait(driver, wait_time).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        # 요소가 뷰포트에 보일 때까지 스크롤
        driver.execute_script("arguments[0].scrollIntoView(true);", element)
        # 요소가 클릭 가능할 때까지 대기
        element = WebDriverWait(driver, wait_time).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        element.click()
        logger.info(f"{element_name} 클릭 완료")
        time.sleep(2)  # 클릭 후 잠시 대기
    except TimeoutException:
        logger.error(f"{element_name} 요소를 찾는 데 시간이 초과되었습니다.")
    except ElementClickInterceptedException:
        logger.error(f"{element_name} 요소를 클릭할 수 없습니다. 다른 요소에 가려져 있을 수 있습니다.")
    except NoSuchElementException:
        logger.error(f"{element_name} 요소를 찾을 수 없습니다.")
    except Exception as e:
        logger.error(f"{element_name} 클릭 중 오류 발생: {e}")
# 차트 클릭하기
def perform_chart_actions(driver):
    # 시간 메뉴 클릭
    click_element_by_xpath(
        driver,
        "/html/body/div[1]/div[2]/div[3]/span/div/div/div[1]/div/div/cq-menu[1]",
        "시간 메뉴"
    )
    # 1시간 옵션 선택
    click_element_by_xpath(
        driver,
        "/html/body/div[1]/div[2]/div[3]/span/div/div/div[1]/div/div/cq-menu[1]/cq-menu-dropdown/cq-item[8]",
        "1시간 옵션"
    )
# 스크린샷 캡쳐 및 base64 이미지 인코딩
def capture_and_encode_screenshot(driver):
    try:
        # 스크린샷 캡처
        png = driver.get_screenshot_as_png()
        # PIL Image로 변환
        img = Image.open(io.BytesIO(png))
        # 이미지가 클 경우 리사이즈 (OpenAI API 제한에 맞춤)
        img.thumbnail((2000, 2000))
        # 이미지를 바이트로 변환
        buffered = io.BytesIO()
        img.save(buffered, format="PNG")
        # base64로 인코딩
        base64_image = base64.b64encode(buffered.getvalue()).decode('utf-8')
        return base64_image
    except Exception as e:
        logger.error(f"스크린샷 캡처 및 인코딩 중 오류 발생: {e}")
        return None

### 메인 AI 트레이딩 로직
def ai_trading():
    global upbit
    ### 데이터 가져오기
    # 1. 현재 투자 상태 조회
    all_balances = upbit.get_balances() # 모든 잔고 조회
    # XRP와 KRW 잔고만 필터링
    filtered_balances = [balance for balance in all_balances if balance['currency'] in ['XRP', 'KRW']]
    
    # 3. XRP 차트 데이터 조회 (시간별, 30개)
    df_daily = pyupbit.get_ohlcv("KRW-XRP", interval="minute60", count=30)
    # print(df_daily) # 디버깅 완료 후 제거 또는 logger.debug(df_daily.to_string()) 등으로 대체

    # 4. 기술적 지표 계산
    sma_20_series = calculate_sma(df_daily.copy(), window=20) # .copy()로 SettingWithCopyWarning 방지
    latest_sma_20 = sma_20_series.iloc[-1] if sma_20_series is not None and not sma_20_series.empty else None

    ichimoku_data = calculate_ichimoku(df_daily.copy()) # .copy()로 SettingWithCopyWarning 방지

    logger.info(f"Latest 20-period SMA: {latest_sma_20}")
    logger.info(f"Ichimoku Data: {ichimoku_data}")

    # 6. strategy.txt 데이터 가져오기
    strategy_text = ""
    try:
        # strategy.txt 파일이 autotrade.py와 같은 디렉토리에 있다고 가정
        # 파일 경로를 절대 경로로 지정하거나, 현재 스크립트 위치 기준으로 설정하는 것이 더 안정적일 수 있습니다.
        with open("strategy.txt", "r", encoding="utf-8") as f:
            strategy_text = f.read()
    except FileNotFoundError:
        logger.error("strategy.txt 파일을 찾을 수 없습니다.")
        return
    except Exception as e:
        logger.error(f"strategy.txt 파일을 읽는 중 오류 발생: {e}")
        return

    # 7. Selenium으로 차트 캡처
    driver = None
    try:
        driver = create_driver()
        driver.get("https://upbit.com/full_chart?code=CRIX.UPBIT.KRW-XRP") # XRP 차트 페이지로 변경
        logger.info("페이지 로드 완료")
        time.sleep(10)  # 페이지 로딩 대기 시간 증가
        logger.info("차트 작업 시작")
        perform_chart_actions(driver)
        logger.info("차트 작업 완료")
        chart_image = capture_and_encode_screenshot(driver)
        logger.info(f"스크린샷 캡처 완료.")
    except WebDriverException as e:
        logger.error(f"캡쳐시 WebDriver 오류 발생: {e}")
        chart_image = None
    except Exception as e:
        logger.error(f"차트 캡처 중 오류 발생: {e}")
        chart_image = None
    finally:
        if driver:
            driver.quit()

    ### AI에게 데이터 제공하고 판단 받기
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    if not client.api_key:
        logger.error("OpenAI API key is missing or invalid.")
        return None
    try:
        # 데이터베이스 연결
        with sqlite3.connect('xrp_trades.db') as conn: # XRP 데이터베이스 사용
            # 최근 거래 내역 가져오기
            recent_trades = get_recent_trades(conn)
            
            # 현재 시장 데이터 수집 (기존 코드에서 가져온 데이터 사용)
            current_market_data = {
                "daily_ohlcv": df_daily.to_dict()
            }
            # AI에 전달할 추가 정보
            technical_indicators_info = f"""
Technical Indicators (based on hourly data):
- 20-period SMA: {latest_sma_20 if latest_sma_20 is not None else 'N/A'}
- Ichimoku Cloud: {json.dumps(ichimoku_data) if ichimoku_data else 'N/A'}"""
            # AI 모델 호출
            response = client.chat.completions.create(
                model="gpt-4o-2024-08-06",
                messages=[
                    {
                        "role": "system",
                        "content": f"""You are an expert in XRP investing. Analyze the provided data and determine whether to buy, sell, or hold at the current moment. Consider the following in your analysis:
                        
                        - Patterns and trends visible in the chart image.
                        - Provided technical indicators: 20-period Simple Moving Average (SMA) and Ichimoku Cloud components (Tenkan-sen, Kijun-sen, current values of Senkou Span A & B, and Chikou Span context - compare current price with lagged price).
                        
                         Trading Strategy:
                        {strategy_text}
                        
                        Based on this trading method, analyze the current market situation and make a judgment by synthesizing it with all provided data, including the technical indicators.
                        
                        Response format:
                        1. Decision (buy, sell, or hold)
                        2. If the decision is 'buy', provide a percentage (1-100) of available KRW to use for buying.
                        If the decision is 'sell', provide a percentage (1-100) of held XRP to sell.
                        If the decision is 'hold', set the percentage to 0.
                        3. Reason for your decision

                        Ensure that the percentage is an integer between 1 and 100 for buy/sell decisions, and exactly 0 for hold decisions.
                        Your percentage should reflect the strength of your conviction in the decision based on the analyzed data."""
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": f"""Current investment status: {json.dumps(filtered_balances)}
                
                Hourly OHLCV (last 30 hours): {df_daily.to_json(orient='split')} 
                {technical_indicators_info}
                
                """
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/png;base64,{chart_image}"
                                }
                            }
                        ]
                    }
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "trading_decision",
                        "strict": True,
                        "schema": {
                            "type": "object",
                            "properties": {
                                "decision": {"type": "string", "enum": ["buy", "sell", "hold"]},
                                "percentage": {"type": "integer"},
                                "reason": {"type": "string"}
                            },
                            "required": ["decision", "percentage", "reason"],
                            "additionalProperties": False
                        }
                    }
                },
                max_tokens=1000
            )

            # Pydantic을 사용하여 AI의 트레이딩 결정 구조를 정의
            try:
                result = TradingDecision.model_validate_json(response.choices[0].message.content)
            except Exception as e:
                logger.error(f"Error parsing AI response: {e}")
                return
            
            logger.info(f"AI Decision: {result.decision.upper()}")
            logger.info(f"Decision Reason: {result.reason}")

            order_executed = False

            if result.decision == "buy":
                my_krw = upbit.get_balance("KRW") # KRW 잔고 조회
                if my_krw is None:
                    logger.error("Failed to retrieve KRW balance.")
                    return
                buy_amount = my_krw * (result.percentage / 100) * 0.9995  # 수수료 고려
                if buy_amount > 5000:
                    logger.info(f"Buy Order Executed for XRP: {result.percentage}% of available KRW")
                    try:
                        order = upbit.buy_market_order("KRW-XRP", buy_amount) # XRP 매수
                        if order:
                            logger.info(f"Buy order executed successfully: {order}")
                            order_executed = True
                        else:
                            logger.error("Buy order failed.")
                    except Exception as e:
                        logger.error(f"Error executing buy order: {e}")
                else:
                    logger.warning("Buy Order Failed: Insufficient KRW (less than 5000 KRW)")
            elif result.decision == "sell":
                my_xrp = upbit.get_balance("KRW-XRP") # XRP 잔고 조회
                if my_xrp is None:
                    logger.error("Failed to retrieve XRP balance.")
                    return
                sell_amount = my_xrp * (result.percentage / 100)
                current_xrp_price = pyupbit.get_current_price("KRW-XRP") # XRP 현재가 조회
                if current_xrp_price is not None and sell_amount * current_xrp_price > 5000: # 최소 주문 금액 (5000 KRW 상당) 확인
                    logger.info(f"Sell Order Executed for XRP: {result.percentage}% of held XRP")
                    try:
                        order = upbit.sell_market_order("KRW-XRP", sell_amount) # XRP 매도
                        if order:
                            order_executed = True
                        else:
                            logger.error("Sell order failed.")
                    except Exception as e:
                        logger.error(f"Error executing sell order: {e}")
                else:
                    logger.warning("Sell Order Failed: Insufficient XRP (less than 5000 KRW worth)")
            
            # 거래 실행 여부와 관계없이 현재 잔고 조회
            time.sleep(2)  # API 호출 제한을 고려하여 잠시 대기
            balances = upbit.get_balances()
            xrp_balance = next((float(balance['balance']) for balance in balances if balance['currency'] == 'XRP'), 0)
            krw_balance = next((float(balance['balance']) for balance in balances if balance['currency'] == 'KRW'), 0)
            xrp_avg_buy_price = next((float(balance['avg_buy_price']) for balance in balances if balance['currency'] == 'XRP'), 0)
            current_xrp_price_for_log = pyupbit.get_current_price("KRW-XRP") # 로깅용 XRP 현재가

            # 거래 기록을 DB에 저장하기
            log_trade(conn, result.decision, result.percentage if order_executed else 0, result.reason,
                    xrp_balance, krw_balance, xrp_avg_buy_price, current_xrp_price_for_log)
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        return

if __name__ == "__main__":
    # 데이터베이스 초기화
    init_db()

    # 중복 실행 방지를 위한 변수
    trading_in_progress = False

    # 트레이딩 작업을 수행하는 함수
    def job():
        global trading_in_progress
        if trading_in_progress:
            logger.warning("Trading job is already in progress, skipping this run.")
            return
        try:
            trading_in_progress = True
            ai_trading()
        except Exception as e:
            logger.error(f"An error occurred: {e}")
        finally:
            trading_in_progress = False


    # 테스트용 바로 실행
    #job()

    # 매시간 03분에 실행 (예: 13:03, 14:03, 15:03 ...)
    schedule.every().hour.at(":03").do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)