import pandas as pd
import pandas_ta as ta
import numpy as np

class FeatureEngineer:
    def __init__(self):
        pass

    def create_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Uses pandas_ta to append SMA, MACD, RSI, Bollinger Bands, ATR, OBV.
        """
        if df.empty or len(df) < 50:
            return df
            
        df = df.copy()
        
        # Trend
        df.ta.sma(length=20, append=True)
        df.ta.sma(length=50, append=True)
        df.ta.macd(append=True)
        
        # Momentum
        df.ta.rsi(length=14, append=True)
        df.ta.stoch(append=True)
        
        # Volatility
        df.ta.bbands(length=20, append=True)
        df.ta.atr(length=14, append=True)
        
        # Volume
        df.ta.obv(append=True)
        if 'Volume' in df.columns:
            df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()
            
        # Clean up NaNs from indicator calculations
        df = df.dropna().reset_index(drop=True)
        return df

    def align_multi_timeframe(self, df_1d: pd.DataFrame, df_1w: pd.DataFrame, df_1m: pd.DataFrame) -> pd.DataFrame:
        """
        Aligns 1W and 1M data to 1D data.
        To prevent Data Leakage, we use the LAST COMPLETED period data for Higher Timeframes,
        which can be approximated by merging on datetime and forward filling.
        """
        if df_1d.empty:
            return pd.DataFrame()
            
        # Set indexes for joining
        df_1d = df_1d.set_index('Open_Time')
        
        if not df_1w.empty:
            df_1w = df_1w.set_index('Open_Time')
            # Add prefix to distinguish columns
            df_1w = df_1w.add_prefix('1W_')
            # Join and forward fill
            df_1d = df_1d.join(df_1w, how='left')
            df_1d.ffill(inplace=True)
            
        if not df_1m.empty:
            df_1m = df_1m.set_index('Open_Time')
            # Add prefix
            df_1m = df_1m.add_prefix('1M_')
            # Join and forward fill
            df_1d = df_1d.join(df_1m, how='left')
            df_1d.ffill(inplace=True)
            
        return df_1d.reset_index()

    def generate_target(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates the Target variable: Log Returns of T+1 Close.
        """
        if df.empty or 'Close' not in df.columns:
            return df
            
        df = df.copy()
        # Log Returns: ln(Close_t / Close_{t-1})
        df['Log_Return'] = np.log(df['Close'] / df['Close'].shift(1))
        
        # Target is T+1 Log Return
        df['Target_Return'] = df['Log_Return'].shift(-1)
        
        # Drop the last row since we don't have its T+1 target
        df = df.dropna(subset=['Target_Return']).reset_index(drop=True)
        return df

    def process_symbol(self, symbol_data: dict) -> pd.DataFrame:
        """
        Orchestrates processing for a single symbol's multi-TF data.
        """
        df_1d = self.create_technical_indicators(symbol_data.get('1D', pd.DataFrame()))
        df_1w = self.create_technical_indicators(symbol_data.get('1W', pd.DataFrame()))
        df_1m = self.create_technical_indicators(symbol_data.get('1M', pd.DataFrame()))
        
        aligned_df = self.align_multi_timeframe(df_1d, df_1w, df_1m)
        final_df = self.generate_target(aligned_df)
        
        return final_df
