import os
import sys
import json
import logging
import time

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), 'weekly_update.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OHLCV_DIR = os.path.join(PROJECT_ROOT, 'OHLCV')
BACKTEST_DIR = os.path.join(PROJECT_ROOT, 'Binance_Futures_Backtest')
BOT_DIR = os.path.dirname(os.path.abspath(__file__))
TARGET_TICKERS_FILE = os.path.join(BOT_DIR, 'target_tickers.json')

def run_weekly_update():
    logger.info("Starting Weekly Update Process...")
    
    # 1. Update OHLCV Data (1 Hour)
    logger.info("Step 1: Updating 1-Hour OHLCV Data...")
    try:
        if OHLCV_DIR not in sys.path:
            sys.path.append(OHLCV_DIR)
            
        from collect_ohlcv import run_1h_update
        run_1h_update()
        logger.info("Data Update Completed.")
        
    except Exception as e:
        logger.error(f"Failed to update OHLCV data: {e}")
        return

    # 2. Run Backtest & Analysis
    logger.info("Step 2: Running Backtest Analysis...")
    try:
        if BACKTEST_DIR not in sys.path:
            sys.path.append(BACKTEST_DIR)
            
        from backtest_engine import run_analysis
        
        # Run analysis for 5x leverage, selecting top 5
        top_tickers = run_analysis(leverage=5, top_n=5)
        
        if top_tickers:
            logger.info(f"Selected Top 5 Tickers: {top_tickers}")
            
            # 3. Update Target Tickers File
            logger.info("Step 3: Updating Target Tickers File...")
            with open(TARGET_TICKERS_FILE, 'w') as f:
                json.dump(top_tickers, f, indent=4)
            
            logger.info("Successfully updated target_tickers.json")
        else:
            logger.warning("No suitable tickers found during analysis.")
            
    except Exception as e:
        logger.error(f"Failed during backtest analysis: {e}")

if __name__ == "__main__":
    run_weekly_update()
