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
from typing import Optional


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
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None

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
                  xrp_krw_price REAL,
                  stop_loss_price REAL,
                  take_profit_price REAL
                  )''')
    conn.commit()
    return conn

# 거래 기록을 DB에 저장하는 함수
def log_trade(conn, decision, percentage, reason, xrp_balance, krw_balance, xrp_avg_buy_price, xrp_krw_price, stop_loss_price, take_profit_price):
    c = conn.cursor()
    timestamp = datetime.now().isoformat()
    c.execute("""INSERT INTO trades 
                 (timestamp, decision, percentage, reason, xrp_balance, krw_balance, xrp_avg_buy_price, xrp_krw_price, stop_loss_price, take_profit_price)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
              (timestamp, decision, percentage, reason, xrp_balance, krw_balance, xrp_avg_buy_price, xrp_krw_price, stop_loss_price, take_profit_price))
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

"""### Selenium 관련 함수
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
"""
### 메인 AI 트레이딩 로직
def check_and_manage_open_positions(conn):
    """
    현재 보유 중인 XRP 포지션에 대해 설정된 손절/익절가에 도달했는지 확인하고 관리합니다.
    손절/익절이 실행되면 True를 반환하고, 그렇지 않으면 False를 반환합니다.
    """
    balances = upbit.get_balances()
    xrp_info = next((item for item in balances if item['currency'] == 'XRP'), None)
    
    current_xrp_price_check = pyupbit.get_current_price("KRW-XRP") # 가격 먼저 조회
    if not current_xrp_price_check:
        logger.error("현재 XRP 가격 조회 실패 (손절/익절 확인 불가 초기 단계).")
        return False

    if xrp_info and float(xrp_info['balance']) * current_xrp_price_check > 100: # 예: 100원 이상 가치의 XRP 보유 시
        current_xrp_on_upbit = float(xrp_info['balance'])
        avg_buy_price_of_current_xrp = float(xrp_info['avg_buy_price'])
        logger.info(f"현재 {current_xrp_on_upbit} XRP 보유 중 (평단가: {avg_buy_price_of_current_xrp}). 손절/익절 확인 중...")

        c = conn.cursor()
        c.execute("""
            SELECT stop_loss_price, take_profit_price, timestamp
            FROM trades
            WHERE (decision = 'buy' OR decision = 'adjust_sl_tp') 
                  AND (stop_loss_price IS NOT NULL OR take_profit_price IS NOT NULL) -- SL 또는 TP가 설정된 경우만
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        last_buy_sl_tp_record = c.fetchone()

        if last_buy_sl_tp_record:
            active_stop_loss, active_take_profit, trade_timestamp = last_buy_sl_tp_record
            
            c.execute("SELECT COUNT(*) FROM trades WHERE decision LIKE 'sell%' AND timestamp > ?", (trade_timestamp,))
            sell_after_last_buy = c.fetchone()[0]
            
            if sell_after_last_buy > 0:
                logger.info(f"SL/TP 설정 이벤트({trade_timestamp}) 이후 매도 기록이 있어, 해당 SL/TP는 비활성화된 것으로 간주합니다.")
                return False
            
            logger.info(f"활성 손절가: {active_stop_loss}, 익절가: {active_take_profit} (설정 시점: {trade_timestamp})")
            
            action_description = ""
            action_type = ""

            if active_stop_loss and current_xrp_price_check <= active_stop_loss:
                action_description = f"손절가 도달 ({active_stop_loss}), 현재가: {current_xrp_price_check}"
                action_type = "sell_stop_loss"
            elif active_take_profit and current_xrp_price_check >= active_take_profit:
                action_description = f"익절가 도달 ({active_take_profit}), 현재가: {current_xrp_price_check}"
                action_type = "sell_take_profit"

            if action_type:
                logger.info(f"{action_type.replace('_', ' ').title()} 조건 충족. 실행: {current_xrp_on_upbit} XRP 전량 매도 시도.")
                if current_xrp_on_upbit * current_xrp_price_check >= 5000:
                    try:
                        order = upbit.sell_market_order("KRW-XRP", current_xrp_on_upbit)
                        if order and order.get('uuid'):
                            logger.info(f"{action_type.replace('_', ' ').title()} 주문 성공: {order}")
                            time.sleep(2)
                            final_balances = upbit.get_balances()
                            xrp_bal = next((float(b['balance']) for b in final_balances if b['currency'] == 'XRP'), 0)
                            krw_bal = next((float(b['balance']) for b in final_balances if b['currency'] == 'KRW'), 0)
                            log_trade(conn, action_type, 100, action_description, xrp_bal, krw_bal, 0, current_xrp_price_check, None, None)
                            return True # 중요: 손절/익절 실행 시 True 반환
                        else: logger.error(f"{action_type.replace('_', ' ').title()} 주문 실패: {order}")
                    except Exception as e: logger.error(f"{action_type.replace('_', ' ').title()} 주문 중 예외: {e}")
                else: logger.warning(f"{action_type.replace('_', ' ').title()} 주문 불가: 최소 주문 금액 미만.")
        else: logger.info("XRP 보유 중이나, 유효한 SL/TP 설정을 찾지 못했습니다.")
    else: logger.info("XRP 미보유 또는 잔고 미미. SL/TP 확인 건너뜀.")
    return False

def ai_trading():
    global upbit
    ### 데이터 가져오기
    # 1. 현재 투자 상태 조회
    all_balances = upbit.get_balances() # 모든 잔고 조회
    # XRP와 KRW 잔고만 필터링
    filtered_balances = [balance for balance in all_balances if balance['currency'] in ['XRP', 'KRW']]

    # 데이터베이스 연결 (함수 초기에 한 번만)
    try:
        conn = init_db() # DB 초기화 및 연결 가져오기
        # --- 손절/익절 조건 우선 확인 ---
        if check_and_manage_open_positions(conn):
            logger.info("손절 또는 익절 실행됨. 이번 AI 트레이딩 사이클을 종료합니다.")
            return # 손절/익절이 실행되었으면 AI 판단 로직을 건너뜀
    except sqlite3.Error as e:
        logger.error(f"Database connection error during SL/TP check: {e}")
        return # DB 오류 시 함수 종료
    finally:
        if conn:
            conn.close() # 여기서 conn을 닫으면 아래 AI 로직에서 다시 열어야 함.
                         # 또는 ai_trading 전체를 try/finally로 감싸고 마지막에 conn.close()

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

    """# 7. Selenium으로 차트 캡처
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
    """
    chart_image = None # Selenium 비활성화로 None 처리
    ### AI에게 데이터 제공하고 판단 받기
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    if not client.api_key:
        logger.error("OpenAI API key is missing or invalid.")
        return None
    try:
        # 데이터베이스 연결
        with init_db() as conn: # init_db()가 연결 객체를 반환하도록 수정되었으므로, 이를 사용
            # 최근 거래 내역 가져오기
            recent_trades = get_recent_trades(conn)
            
            # 현재 시장 데이터 수집 (기존 코드에서 가져온 데이터 사용)
            current_market_data = {
                "daily_ohlcv": df_daily.to_dict()
            }

            # AI에게 전달할 사용자 메시지 콘텐츠 구성
            user_message_parts = [
                {
                    "type": "text",
                    "text": f"""Current investment status: {json.dumps(filtered_balances)}
                
Hourly OHLCV (last 30 hours): {df_daily.to_json(orient='split')}
{technical_indicators_info}"""
                }
            ]
            if chart_image:
                user_message_parts.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{chart_image}"}
                })
            
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
                        
                        {'- Patterns and trends visible in the chart image.' if chart_image else ''}
                        - Provided technical indicators: 20-period Simple Moving Average (SMA) and Ichimoku Cloud components (Tenkan-sen, Kijun-sen, current values of Senkou Span A & B, and Chikou Span context - compare current price with lagged price).
                        
                         Trading Strategy:
                        {strategy_text}
                        
                        Based on this trading method, analyze the current market situation and make a judgment by synthesizing it with all provided data, including the technical indicators.
                        
                        Response format:
                        1. Decision (buy, sell, or hold)
                        2. If the decision is 'buy', provide a percentage (1-100) of available KRW to use for buying.
                           If 'sell', provide a percentage (1-100) of held XRP to sell.
                           If 'hold', set percentage to 0.
                        3. Reason for your decision.
                        4. stop_loss_price (float or null): The calculated stop-loss price if the decision is 'buy', based on the provided strategy. Set to null if 'sell' or 'hold'.
                        5. take_profit_price (float or null): The calculated take-profit price if the decision is 'buy', based on the provided strategy. Set to null if 'sell' or 'hold'.

                        If you are currently holding XRP (check 'Current investment status') and your main decision is 'hold':
                        Review the existing stop-loss and take-profit levels. Based on the 'Trading Strategy' (especially section 4.2 on multi-stage exits and SL adjustments like moving to breakeven) and recent price action, determine if the stop_loss_price and/or take_profit_price for the current open position should be adjusted.
                        If an adjustment is recommended for a 'hold' decision with an open position, provide the new 'stop_loss_price' and 'take_profit_price' in these fields.
                        If no adjustment is needed for a 'hold' decision with an open position, you can set these fields to null or reflect the current unchanged SL/TP.
                        For 'buy' decisions, 'stop_loss_price' and 'take_profit_price' are for the new position.
                        For 'sell' decisions, 'stop_loss_price' and 'take_profit_price' should generally be null.

                        Ensure that the percentage is an integer. For buy/sell decisions, it should be between 1 and 100. For hold decisions, it must be 0.
                        Your percentage should reflect the strength of your conviction in the decision based on the analyzed data."""
                    },
                    {
                        "role": "user",
                        "content": user_message_parts
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
                                "reason": {"type": "string"},
                                "stop_loss_price": {"type": ["number", "null"]},
                                "take_profit_price": {"type": ["number", "null"]}
                            },
                            "required": ["decision", "percentage", "reason", "stop_loss_price", "take_profit_price"],
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
            if result.decision == "buy":
                logger.info(f"AI Suggested Initial Stop-Loss: {result.stop_loss_price}")
                logger.info(f"AI Suggested Initial Take-Profit: {result.take_profit_price}")
            elif result.decision == "hold":
                # 'hold' 결정 시 SL/TP 조정 제안이 있었는지 확인
                current_xrp_balance_check = next((float(b['balance']) for b in filtered_balances if b['currency'] == 'XRP'), 0)
                if current_xrp_balance_check > 0: # XRP를 보유 중일 때만 조정 의미 있음
                    if result.stop_loss_price is not None or result.take_profit_price is not None:
                        # 현재 활성화된 SL/TP 조회
                        c = conn.cursor()
                        c.execute("""
                            SELECT stop_loss_price, take_profit_price, timestamp
                            FROM trades
                            WHERE (decision = 'buy' OR decision = 'adjust_sl_tp')
                                  AND (stop_loss_price IS NOT NULL OR take_profit_price IS NOT NULL)
                            ORDER BY timestamp DESC
                            LIMIT 1
                        """)
                        latest_sl_tp_record = c.fetchone()

                        active_sl = None
                        active_tp = None

                        if latest_sl_tp_record:
                            sl_from_db, tp_from_db, sl_tp_timestamp = latest_sl_tp_record
                            c.execute("SELECT COUNT(*) FROM trades WHERE decision LIKE 'sell%' AND timestamp > ?", (sl_tp_timestamp,))
                            sell_after_event = c.fetchone()[0]
                            if sell_after_event == 0: # 해당 SL/TP가 설정된 포지션이 아직 유효함
                                active_sl = sl_from_db
                                active_tp = tp_from_db
                        
                        new_sl_suggested = result.stop_loss_price
                        new_tp_suggested = result.take_profit_price

                        final_sl_to_log = active_sl
                        final_tp_to_log = active_tp
                        made_favorable_adjustment = False

                        if new_sl_suggested is not None:
                            if active_sl is None or new_sl_suggested > active_sl: # 새 SL이 더 높거나 기존 SL이 없으면
                                final_sl_to_log = new_sl_suggested
                                made_favorable_adjustment = True
                        
                        if new_tp_suggested is not None:
                            if active_tp is None or new_tp_suggested > active_tp: # 새 TP가 더 높거나 기존 TP가 없으면
                                final_tp_to_log = new_tp_suggested
                                made_favorable_adjustment = True
                        
                        if made_favorable_adjustment and (final_sl_to_log != active_sl or final_tp_to_log != active_tp):
                            logger.info(f"AI Suggested FAVORABLE SL/TP Adjustment on HOLD. Active: (SL:{active_sl}, TP:{active_tp}), Suggested: (SL:{new_sl_suggested}, TP:{new_tp_suggested}), Final to log: (SL:{final_sl_to_log}, TP:{final_tp_to_log})")
                            adjusted_balances = upbit.get_balances()
                            adj_xrp_bal = next((float(b['balance']) for b in adjusted_balances if b['currency'] == 'XRP'), 0)
                            adj_krw_bal = next((float(b['balance']) for b in adjusted_balances if b['currency'] == 'KRW'), 0)
                            adj_xrp_avg_b_p = next((float(b['avg_buy_price']) for b in adjusted_balances if b['currency'] == 'XRP'), 0)
                            adj_current_xrp_price = pyupbit.get_current_price("KRW-XRP")

                            log_trade(conn, "adjust_sl_tp", 0, f"AI SL/TP favorable adjustment. New SL={final_sl_to_log}, New TP={final_tp_to_log}. Reason: {result.reason}",
                                      adj_xrp_bal, adj_krw_bal, adj_xrp_avg_b_p, adj_current_xrp_price,
                                      final_sl_to_log, final_tp_to_log)
                        else:
                            logger.info(f"AI Decision: HOLD. Suggested SL/TP (SL:{new_sl_suggested}, TP:{new_tp_suggested}) not more favorable than active (SL:{active_sl}, TP:{active_tp}) or no actual change. No adjustment logged.")

                        # AI의 주 결정은 'hold'이므로, 이후 매매 로직은 타지 않도록 함.
                        # logged_percentage 등은 아래에서 일반적인 'hold'로 처리됨.
                    else:
                        logger.info("AI Decision: HOLD, no SL/TP adjustment suggested for open position.")
                else:
                    logger.info("AI Decision: HOLD, no open XRP position to adjust SL/TP for.")
            
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

            # AI가 제안한 손절가 및 익절가 (매수 시에만 의미 있음)
            stop_loss_to_log = result.stop_loss_price if result.decision == "buy" else None
            take_profit_to_log = result.take_profit_price if result.decision == "buy" else None

            # 'adjust_sl_tp'는 위에서 이미 특별 처리하여 로깅했으므로, 여기서는 일반적인 buy/sell/hold만 로깅
            if result.decision != "hold" or (result.stop_loss_price is None and result.take_profit_price is None and current_xrp_balance_check > 0) or current_xrp_balance_check == 0 : # 순수 hold 또는 조정 없는 hold
                logged_percentage = result.percentage
                # 매수/매도 주문이 실제 실행되지 않았으면 percentage를 0으로 로깅 (단, hold는 원래 0)
                if not order_executed and result.decision not in ["hold", "adjust_sl_tp"]:
                    logged_percentage = 0
                
                # 'buy'가 아닌 경우 SL/TP는 None으로 설정 (이미 위에서 stop_loss_to_log, take_profit_to_log가 처리)
                log_trade(conn, result.decision, logged_percentage, result.reason,
                        xrp_balance, krw_balance, xrp_avg_buy_price, current_xrp_price_for_log, stop_loss_to_log, take_profit_to_log)

    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        # conn이 여기서 close 되므로, 만약 위에서 conn을 열었다면 여기서 닫히게 됨.
        # check_and_manage_open_positions 와의 conn 관리 일관성 필요.
        return 

if __name__ == "__main__":
    # 데이터베이스 파일이 없으면 생성됨
    conn_init = init_db() 
    conn_init.close() # 초기화만 하고 닫음

    trading_in_progress = False

    def job():
        global trading_in_progress
        if trading_in_progress:
            logger.warning("이전 트레이딩 작업이 아직 진행 중입니다. 이번 실행은 건너<0xEB><08><0x81>니다.")
            return
        logger.info("AI 트레이딩 작업을 시작합니다...")
        try:
            trading_in_progress = True
            ai_trading()
            logger.info("AI 트레이딩 작업 완료.")
        except Exception as e:
            logger.error(f"ai_trading 중 예외 발생: {e}", exc_info=True)
        finally:
            trading_in_progress = False

    # # 테스트용 즉시 실행
    # job()

    logger.info("스케줄러 시작. 매시간 3분에 거래 로직 실행.")
    schedule.every().hour.at(":03").do(job)
    
    # 프로그램 시작 시 한 번 즉시 실행 (선택 사항)
    # logger.info("프로그램 시작 시 첫 트레이딩 작업 실행...")
    # job()

    while True:
        schedule.run_pending()
        time.sleep(1)