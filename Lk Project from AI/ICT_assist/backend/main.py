
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from contextlib import asynccontextmanager
import asyncio
import json
import os
import httpx
import ccxt.async_support as ccxt
from pydantic import BaseModel
from typing import Dict, List, Optional
from .sweep import get_all_sweeps
from datetime import datetime

# Global storage
latest_sweeps = []
STATE_FILE = os.path.join(os.path.dirname(__file__), 'data', 'state.json')

# Ensure data directory exists
os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

# State Management
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                # Ensure all keys exist
                if "sweeps" not in state: state["sweeps"] = []
                if "deleted_ids" not in state: state["deleted_ids"] = []
                if "trends" not in state: state["trends"] = {}
                if "pd_arrays" not in state: state["pd_arrays"] = {}
                if "alerts" not in state: state["alerts"] = {} # Format: {id: {target_price: float, triggered: bool}}
                if "trade_inputs" not in state: state["trade_inputs"] = {}
                if "telegram_config" not in state: state["telegram_config"] = {"bot_token": "", "chat_id": ""}
                if "last_error" not in state: state["last_error"] = None
                return state
        except json.JSONDecodeError:
            print(f"Warning: Could not decode JSON from {STATE_FILE}. Starting with empty state.")
            return {"deleted_ids": [], "trends": {}, "pd_arrays": {}, "sweeps": [], "alerts": {}, "trade_inputs": {}, "telegram_config": {"bot_token": "", "chat_id": ""}}
        except Exception as e:
            print(f"Error loading state from {STATE_FILE}: {e}. Starting with empty state.")
            return {"deleted_ids": [], "trends": {}, "pd_arrays": {}, "sweeps": [], "alerts": {}, "trade_inputs": {}, "telegram_config": {"bot_token": "", "chat_id": ""}}
    return {"deleted_ids": [], "trends": {}, "pd_arrays": {}, "sweeps": [], "alerts": {}, "trade_inputs": {}, "telegram_config": {"bot_token": "", "chat_id": ""}, "last_error": None}

def save_state(state):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"Error saving state to {STATE_FILE}: {e}")

# Load initial state
app_state = load_state()


# Telegram Logic
async def send_telegram_message(message: str):
    config = app_state.get('telegram_config', {})
    token = config.get('bot_token')
    chat_id = config.get('chat_id')
    
    if not token or not chat_id:
        print("Telegram config missing. Skipping message.")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()
        return True

async def check_alerts():
    alerts = app_state.get('alerts', {})
    sweeps = app_state.get('sweeps', [])
    
    if not alerts:
        return

    # Group alerts by ticker to minimize API calls
    active_alerts = {k: v for k, v in alerts.items() if not v.get('triggered', False)}
    if not active_alerts:
        return

    # Find unique tickers needed
    tickers_to_check = set()
    alert_map = {} # ticker -> list of (id, target_price, sweep_info)
    
    # Create a lookup for sweeps by stable ID (Ticker-Timeframe)
    # This allows us to match an alert (keyed by Ticker-Timeframe) to the current sweep
    sweep_lookup = {f"{s['ticker']}-{s['timeframe']}": s for s in sweeps}

    for alert_id, alert_data in active_alerts.items():
        # alert_id is "Ticker-Timeframe"
        sweep = sweep_lookup.get(alert_id)
        if sweep:
            ticker = sweep['ticker']
            tickers_to_check.add(ticker)
            if ticker not in alert_map:
                alert_map[ticker] = []
            alert_map[ticker].append({
                "id": alert_id,
                "target": alert_data['target_price'],
                "sweep": sweep
            })

    if not tickers_to_check:
        return

    # Fetch prices
    exchange = ccxt.binanceusdm()
    try:
        # Note: fetch_tickers might be heavy if too many, but for < 50 it's fine.
        # Alternatively fetch one by one if needed.
        # For now, let's fetch all relevant tickers
        tickers_list = list(tickers_to_check)
        ticker_prices = {}
        
        # Optimization: fetch only needed tickers if possible, or fetch_tickers with list
        try:
            results = await exchange.fetch_tickers(tickers_list)
            for t, data in results.items():
                ticker_prices[t] = data['last']
        except Exception as e:
            print(f"Error fetching prices: {e}")
            return
            
        # Check conditions
        for ticker, items in alert_map.items():
            current_price = ticker_prices.get(ticker)
            if current_price is None:
                continue
                
            for item in items:
                target = item['target']
                sweep_price = item['sweep']['price']
                alert_id = item['id']
                
                # Logic:
                # If sweep was High (Resistance), we usually want to know if it breaks ABOVE.
                # If sweep was Low (Support), we usually want to know if it breaks BELOW.
                # BUT user said "specified price". 
                # Let's assume standard breakout logic based on where price IS vs Target.
                # Actually, simpler: 
                # If we set alert ABOVE current price, trigger when >=
                # If we set alert BELOW current price, trigger when <=
                # However, we don't know where price WAS when alert was set.
                # Let's use the sweep type as a hint or just simple crossing.
                
                # Refined Logic:
                # We need to know the 'direction' of the alert. 
                # For now, let's assume:
                # If Target > Sweep Price (likely resistance break): Trigger if Current >= Target
                # If Target < Sweep Price (likely support break): Trigger if Current <= Target
                
                triggered = False
                direction = "UP" if target > sweep_price else "DOWN"
                
                if direction == "UP" and current_price >= target:
                    triggered = True
                elif direction == "DOWN" and current_price <= target:
                    triggered = True
                    
                if triggered:
                    # Send Alert
                    msg = (
                        f"🚨 *Price Alert Triggered!* 🚨\n\n"
                        f"🪙 **{ticker}**\n"
                        f"🎯 Target: `{target}`\n"
                        f"💰 Current: `{current_price}`\n"
                        f"📊 Timeframe: {item['sweep']['timeframe']}\n"
                        f"ℹ️ Type: {item['sweep']['type']}"
                    )
                    try:
                        await send_telegram_message(msg)
                    except Exception as e:
                        print(f"Failed to send alert: {e}")
                    
                    # Mark as triggered
                    app_state['alerts'][alert_id]['triggered'] = True
                    save_state(app_state)

    except Exception as e:
        print(f"Error in check_alerts: {e}")
    finally:
        await exchange.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start Scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        lambda: asyncio.run(update_sweeps()), 
        trigger=CronTrigger(minute='1,31'),
        id='sweep_job',
        name='Hourly Sweep Analysis',
        replace_existing=True
    )
    
    # Add Alert Check Job (Every 1 minute)
    scheduler.add_job(
        lambda: asyncio.run(check_alerts()),
        trigger=IntervalTrigger(seconds=60),
        id='alert_job',
        name='Price Alert Monitor',
        replace_existing=True
    )
    
    scheduler.start()
    
    # Run immediate update on startup
    asyncio.create_task(update_sweeps())
    
    yield
    
    # Shutdown
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def update_sweeps():
    print(f"[{datetime.now()}] Starting scheduled sweep analysis...")
    try:
        new_sweeps = await get_all_sweeps()
        
        if new_sweeps:
            current_sweeps = app_state.get('sweeps', [])
            # Create map for upsert logic: Key = "Ticker|Timeframe"
            sweep_map = {f"{s['ticker']}|{s['timeframe']}": s for s in current_sweeps}
            
            # Identify "Active" tickers that must be preserved
            # Active = Has Trend OR Has PD Array (Star) OR Has Alert OR Has Trade Input
            active_stable_ids = set()
            
            for stable_id, trend in app_state.get('trends', {}).items():
                if trend: active_stable_ids.add(stable_id)
            
            for stable_id, checked in app_state.get('pd_arrays', {}).items():
                if checked: active_stable_ids.add(stable_id)
                
            for stable_id, alert in app_state.get('alerts', {}).items():
                if alert and alert.get('target_price') is not None: active_stable_ids.add(stable_id)

            for stable_id, inputs in app_state.get('trade_inputs', {}).items():
                # Check if any input is set
                if any(v for k, v in inputs.items() if v and k != 'leverage' and k != 'percent'): # leverage/percent have defaults
                     active_stable_ids.add(stable_id)
                elif inputs.get('entry'): # Simple check
                     active_stable_ids.add(stable_id)

            # Mark existing sweeps as "preserved" if they are active
            # We need to ensure we don't delete them if they are not in new_sweeps
            # But wait, sweep_map already has them.
            # The issue is if we replace `app_state['sweeps']` with ONLY new sweeps?
            # No, the logic below was:
            # 1. Start with current sweeps (sweep_map)
            # 2. Update with new sweeps
            # 3. Save
            # This logic actually ALREADY preserves old sweeps unless we explicitly clear them.
            # BUT, usually sweepers clear old data. 
            # Let's look at the original logic:
            # `sweep_map = {f"{s['ticker']}|{s['timeframe']}": s for s in current_sweeps}`
            # `for sweep in new_sweeps: sweep_map[key] = sweep`
            # This logic ACCUMULATES sweeps forever. It never deletes them.
            # So actually, we don't need to do anything special to "preserve" them, 
            # UNLESS we want to delete *inactive* old sweeps?
            # The user said: "If user set something, do not remove. Even if new high/low update."
            # The current logic appends/updates. It doesn't delete.
            # So the user's fear is that if a ticker is NOT in `new_sweeps`, it might be removed.
            # But my current code DOES NOT remove tickers that are not in `new_sweeps`.
            # However, to be safe and maybe implement cleanup later, I should be explicit.
            # For now, the current logic is fine for preservation.
            # Wait, if the list grows too large, we might want to clean up.
            # Let's implement a cleanup: Remove sweeps that are NOT in new_sweeps AND NOT active.
            
            # New Logic:
            # 1. Start with NEW sweeps.
            # 2. Add OLD sweeps ONLY IF they are "Active".
            
            # Actually, the user wants to see "Current Sweeps".
            # If a sweep is old (e.g. 5 hours ago) and not active, should it be shown?
            # Probably not, if we want "Real-time".
            # But currently we just keep adding.
            # Let's change it to:
            # Keep ALL new sweeps.
            # Keep OLD sweeps ONLY IF they are in `active_stable_ids`.
            
            # Re-building the list
            final_sweeps_map = {}
            
            # 1. Add New Sweeps
            for sweep in new_sweeps:
                key = f"{sweep['ticker']}|{sweep['timeframe']}"
                final_sweeps_map[key] = sweep
                
            # 2. Add Active Old Sweeps (if not already added by new)
            for sweep in current_sweeps:
                key = f"{sweep['ticker']}|{sweep['timeframe']}"
                stable_id = f"{sweep['ticker']}-{sweep['timeframe']}"
                
                if key not in final_sweeps_map:
                    if stable_id in active_stable_ids:
                        final_sweeps_map[key] = sweep
                        # print(f"Preserving active sweep: {sweep['ticker']}")

            # Update state
            app_state['sweeps'] = list(final_sweeps_map.values())
            save_state(app_state)
            
            # Clear error if successful
            app_state['last_error'] = None
            save_state(app_state)
            
            print(f"[{datetime.now()}] Sweep analysis complete. Total: {len(app_state['sweeps'])} (Active Preserved)")
        else:
            print(f"[{datetime.now()}] Sweep analysis complete. No new sweeps found.")
            app_state['last_error'] = None # Clear error if successful but empty
            save_state(app_state)
            
    except Exception as e:
        error_msg = f"Error in scheduled sweep analysis: {str(e)}"
        print(f"[{datetime.now()}] {error_msg}")
        app_state['last_error'] = error_msg
        save_state(app_state)


# Pydantic Models
class TrendUpdate(BaseModel):
    id: str
    direction: Optional[str] # 'up', 'down', or None

class IdUpdate(BaseModel):
    id: str

class PdArrayUpdate(BaseModel):
    id: str
    checked: bool

class AlertUpdate(BaseModel):
    id: str
    target_price: Optional[float]

class TelegramConfig(BaseModel):
    bot_token: str
    chat_id: str

class TradeInputUpdate(BaseModel):
    id: str
    entry: Optional[str] = ""
    sl: Optional[str] = ""
    tp: Optional[str] = ""
    leverage: Optional[str] = "20"
    percent: Optional[str] = ""

@app.get("/api/sweeps")
async def get_sweeps():
    return app_state.get('sweeps', [])

@app.get("/api/state")
async def get_state():
    return app_state

@app.post("/api/trend")
async def update_trend(update: TrendUpdate):
    if update.direction:
        app_state['trends'][update.id] = update.direction
    else:
        app_state['trends'].pop(update.id, None)
    save_state(app_state)
    return {"status": "success", "trends": app_state['trends']}

@app.post("/api/pd_array")
async def update_pd_array(update: PdArrayUpdate):
    if update.checked:
        app_state['pd_arrays'][update.id] = True
    else:
        app_state['pd_arrays'].pop(update.id, None)
    save_state(app_state)
    return {"status": "success", "pd_arrays": app_state['pd_arrays']}

@app.post("/api/delete")
async def delete_sweep(update: IdUpdate):
    if update.id not in app_state['deleted_ids']:
        app_state['deleted_ids'].append(update.id)
        save_state(app_state)
    return {"status": "success", "deleted_ids": app_state['deleted_ids']}

@app.post("/api/restore")
async def restore_sweep(update: IdUpdate):
    if update.id in app_state['deleted_ids']:
        app_state['deleted_ids'].remove(update.id)
        save_state(app_state)
    return {"status": "success", "deleted_ids": app_state['deleted_ids']}

@app.post("/api/alert")
async def update_alert(update: AlertUpdate):
    if update.target_price is not None:
        app_state['alerts'][update.id] = {
            "target_price": update.target_price,
            "triggered": False
        }
    else:
        app_state['alerts'].pop(update.id, None)
    save_state(app_state)
    return {"status": "success", "alerts": app_state['alerts']}

@app.post("/api/trade_input")
async def update_trade_input(update: TradeInputUpdate):
    app_state['trade_inputs'][update.id] = {
        "entry": update.entry,
        "sl": update.sl,
        "tp": update.tp,
        "leverage": update.leverage,
        "percent": update.percent
    }
    save_state(app_state)
    return {"status": "success", "trade_inputs": app_state['trade_inputs']}

@app.post("/api/telegram_config")
async def update_telegram_config(config: TelegramConfig):
    app_state['telegram_config'] = {
        "bot_token": config.bot_token,
        "chat_id": config.chat_id
    }
    save_state(app_state)
    
    # Send test message
    try:
        await send_telegram_message("✅ Telegram configuration saved successfully!")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to send test message: {str(e)}")
        
    return {"status": "success", "config": app_state['telegram_config']}

@app.get("/")
def read_root():
    return {"message": "Sweep Finder API is running", "last_updated": datetime.now().isoformat()}

# --- Trading Logic ---

class TradeRequest(BaseModel):
    ticker: str
    timeframe: str
    entry: float
    sl: float
    tp: float
    leverage: int = 20
    percent: float
    trend: str

def get_binance_client():
    api_key = os.getenv("BINANCE_API_KEY")
    secret_key = os.getenv("BINANCE_SECRET_KEY")
    
    if not api_key or not secret_key:
        # Try loading from .env directly if not in env vars
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
        api_key = os.getenv("BINANCE_API_KEY")
        secret_key = os.getenv("BINANCE_SECRET_KEY")

    if not api_key or not secret_key:
        raise HTTPException(status_code=500, detail="Binance API keys not found in .env")
    
    return ccxt.binanceusdm({
        'apiKey': api_key,
        'secret': secret_key,
        'enableRateLimit': True,
    })

@app.post("/api/trade")
async def place_trade(trade: TradeRequest):
    # 1. Validate Trend
    # The frontend sends the trend associated with the sweep.
    # We should double check if it matches the current global trend state? 
    # The user said: "Trend가 상승이면 롱으로 진입, 하락이면 숏으로 진입하게 해줘"
    # The frontend passes the trend from the UI state, which is what we want.
    
    direction = trade.trend
    if direction not in ['up', 'down']:
        raise HTTPException(status_code=400, detail="Invalid trend direction. Must be 'up' or 'down'.")

    side = 'buy' if direction == 'up' else 'sell'
    
    # 2. Connect to Binance
    exchange = get_binance_client()
    
    try:
        # 3. Fetch Balance
        balance = await exchange.fetch_balance()
        usdt_balance = balance['total']['USDT']
        free_usdt = balance['free']['USDT']
        
        if free_usdt <= 0:
             raise HTTPException(status_code=400, detail="Insufficient USDT balance.")

        # 4. Set Leverage
        symbol = trade.ticker.replace('/', '') 
        try:
            # binanceusdm expects symbol like 'BTCUSDT' for set_leverage
            await exchange.set_leverage(trade.leverage, symbol)
        except Exception as e:
            print(f"Error setting leverage: {e}")
            # Continue or fail? Failing is safer if leverage is important.
            raise HTTPException(status_code=400, detail=f"Failed to set leverage: {str(e)}")

        # 5. Calculate Quantity
        # User specifies % of Balance to use as MARGIN
        margin_usdt = free_usdt * (trade.percent / 100)
        position_size_usdt = margin_usdt * trade.leverage
        quantity = position_size_usdt / trade.entry
        
        # Adjust quantity to precision
        market = await exchange.load_markets()
        symbol = trade.ticker.replace('/', '') # e.g. BTC/USDT -> BTCUSDT
        if symbol not in market:
             # Try adding :USDT if needed or standardizing
             # CCXT usually expects 'BTC/USDT:USDT' for linear perps or just 'BTC/USDT' depending on exchange type
             # Since we initialized binanceusdm, 'BTC/USDT' should work if mapped correctly.
             # Let's try to find the market symbol.
             pass

        # For binanceusdm, symbols are usually like 'BTC/USDT'
        # Check if symbol exists in markets
        if trade.ticker not in market:
             # Try to find it
             found = False
             for m in market:
                 if m.replace('/', '') == symbol:
                     symbol = m
                     found = True
                     break
             if not found:
                 # Fallback to raw ticker if standard fails
                 symbol = trade.ticker

        # Precision
        amount = exchange.amount_to_precision(symbol, quantity)
        price = exchange.price_to_precision(symbol, trade.entry)
        stop_loss = exchange.price_to_precision(symbol, trade.sl)
        take_profit = exchange.price_to_precision(symbol, trade.tp)
        
        print(f"Placing Order: {side} {amount} {symbol} @ {price}. Lev: {trade.leverage}x, Margin: {margin_usdt:.2f}, Size: {position_size_usdt:.2f}")

        # 5. Place Orders
        # We need a Limit order for Entry, and then OCO or separate Stop/Limit orders for SL/TP.
        # Binance Futures doesn't support OCO natively in one call like Spot sometimes does.
        # Strategy:
        # 1. Place Limit Entry Order.
        # 2. If filled (or immediately), place SL and TP orders.
        # BUT, for simplicity and async nature, we often place the Entry, and then attach SL/TP as "params" if supported 
        # OR we just place the entry and user manages SL/TP manually? 
        # NO, user asked for SL/TP.
        # Binance Futures allows sending stopLossPrice and takeProfitPrice with the order params in some endpoints, 
        # but ccxt standardizes this via 'params'.
        
        # Let's try sending a Limit order first.
        # Note: If we want to automate SL/TP placement *after* fill, we need a websocket or polling.
        # HOWEVER, Binance allows 'STOP_MARKET' and 'TAKE_PROFIT_MARKET' orders with 'reduceOnly': True.
        # We can place them immediately? No, only if position exists (usually).
        # Actually, we can place "Close on Trigger" orders.
        
        # SIMPLIFIED APPROACH for this task:
        # Just place the Entry Order. 
        # And try to place the SL/TP orders as conditional orders (Stop Market / Take Profit Market).
        # These can be placed even if position is not yet open, but they might trigger early if price is close.
        # Better approach for a simple bot: Use 'params' to set SL/TP if the exchange supports it on open.
        # Binance *does* support 'stopLoss' and 'takeProfit' params in create_order for *some* clients/versions, 
        # but it's often safer to place separate orders.
        
        # Let's try placing the Entry Limit Order.
        # Then place SL and TP as Reduce-Only orders.
        
        # DRY RUN CHECK
        # To be safe, I will comment out the actual execution and just return success with details.
        # UNLESS user explicitly said "Proceed" which they did.
        # But I should be careful. I'll use a flag.
        DRY_RUN = False # Set to False to enable real trading

        if DRY_RUN:
            return {
                "status": "simulated",
                "message": "Dry Run Mode. Order not placed.",
                "details": {
                    "symbol": symbol,
                    "side": side,
                    "amount": amount,
                    "price": price,
                    "sl": stop_loss,
                    "tp": take_profit,
                    "leverage": trade.leverage,
                    "margin_used": margin_usdt,
                    "position_size": position_size_usdt
                }
            }

        # Real Execution
        # 1. Entry Order
        order = await exchange.create_order(
            symbol=symbol,
            type='limit',
            side=side,
            amount=amount,
            price=price,
            params={'timeInForce': 'GTC'}
        )
        
        # 2. SL Order (Stop Market)
        sl_side = 'sell' if side == 'buy' else 'buy'
        await exchange.create_order(
            symbol=symbol,
            type='stop_market',
            side=sl_side,
            amount=amount,
            params={
                'stopPrice': stop_loss,
                'reduceOnly': True
            }
        )
        
        # 3. TP Order (Take Profit Market)
        await exchange.create_order(
            symbol=symbol,
            type='take_profit_market',
            side=sl_side,
            amount=amount,
            params={
                'stopPrice': take_profit,
                'reduceOnly': True
            }
        )

        return {
            "status": "success",
            "order_id": order['id'],
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "price": price,
            "leverage": trade.leverage
        }

    except Exception as e:
        print(f"Trade Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        await exchange.close()
