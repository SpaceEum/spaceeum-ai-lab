import os
import pandas as pd
import glob
from typing import Dict

class DataLoader:
    def __init__(self, root_dir: str = "G:/내 드라이브/LK Project/Lk Project from AI/OHLCV"):
        self.root_dir = root_dir
        self.intervals = ['1D', '1W', '1M']
        
    def _get_folder_name(self, interval: str) -> str:
        interval_map = {
            '1D': '1Day',
            '1W': '1Week',
            '1M': '1Month'
        }
        mapped_interval = interval_map.get(interval, interval)
        return f"Binance_Fureres_USDT_{mapped_interval}_ohlcv"

    def _get_files_for_interval(self, interval: str) -> list:
        folder_path = os.path.join(self.root_dir, self._get_folder_name(interval))
        return glob.glob(os.path.join(folder_path, "*.csv"))

    def load_symbol_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """
        Load 1D, 1W, 1M data for a specific symbol.
        Returns a dictionary: {'1D': df, '1W': df, '1M': df}
        """
        data = {}
        for interval in self.intervals:
            folder_path = os.path.join(self.root_dir, self._get_folder_name(interval))
            file_path = os.path.join(folder_path, f"{symbol}.csv")
            
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                # Standardize 'Open time' to 'Open_Time' if it exists
                if 'Open time' in df.columns:
                    df = df.rename(columns={'Open time': 'Open_Time'})
                
                # Assume columns are roughly: ['Open_Time', 'Open', 'High', 'Low', 'Close', 'Volume']
                if 'Open_Time' in df.columns:
                    df['Open_Time'] = pd.to_datetime(df['Open_Time'])
                    df = df.sort_values('Open_Time').reset_index(drop=True)
                
                data[interval] = df
            else:
                data[interval] = pd.DataFrame() # Return empty df if not found
                
        return data

    def get_available_symbols(self) -> list:
        """
        Scans the 1D folder to get a list of all available symbols.
        """
        files = self._get_files_for_interval('1D')
        symbols = [os.path.basename(f).replace('.csv', '') for f in files]
        return sorted(symbols)
