import os
import joblib
import pandas as pd
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import RobustScaler

class ModelTrainer:
    def __init__(self, model_save_path: str = "./models"):
        self.model_save_path = model_save_path
        os.makedirs(self.model_save_path, exist_ok=True)
        self.scaler = RobustScaler()
        self.model = lgb.LGBMRegressor(n_estimators=100, learning_rate=0.05, random_state=42)
        
    def prepare_data(self, df: pd.DataFrame):
        # Drop columns not used for training
        if df.empty:
            return None, None
            
        drop_cols = ['Open_Time', 'Target_Return']
        available_drop_cols = [col for col in drop_cols if col in df.columns]
        
        X = df.drop(columns=available_drop_cols)
        
        # Additional cleanup required before passing to model:
        # Convert datetime objects in upper timeframes if they leaked through
        for col in X.columns:
            if 'Time' in col or X[col].dtype == 'datetime64[ns]':
                X = X.drop(columns=[col])
                
        # Fill NaNs from rolling/shifting with median
        X = X.fillna(X.median())
        
        y = df['Target_Return']
        return X, y

    def train(self, df: pd.DataFrame, symbol: str):
        X, y = self.prepare_data(df)
        if X is None or len(X) < 100:
            print(f"Not enough data to train {symbol}")
            return False
            
        # Split (time-series split, no shuffling to avoid leakage)
        split_idx = int(len(X) * 0.8)
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
        
        # Scale
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train
        self.model.fit(
            X_train_scaled, y_train,
            eval_set=[(X_test_scaled, y_test)],
            callbacks=[lgb.early_stopping(stopping_rounds=10)]
        )
        
        # Save artifacts
        model_file = os.path.join(self.model_save_path, f"{symbol}_lgb.pkl")
        scaler_file = os.path.join(self.model_save_path, f"{symbol}_scaler.pkl")
        
        joblib.dump(self.model, model_file)
        joblib.dump(self.scaler, scaler_file)
        print(f"Model saved for {symbol}")
        return True

    def train_incremental(self, df: pd.DataFrame, symbol: str, recent_n_days: int = 30):
        """
        Loads the existing model and scaler, and trains only on the most recent N days of data.
        Updates the existing model's trees.
        """
        model_file = os.path.join(self.model_save_path, f"{symbol}_lgb.pkl")
        scaler_file = os.path.join(self.model_save_path, f"{symbol}_scaler.pkl")
        
        if not os.path.exists(model_file) or not os.path.exists(scaler_file):
            print(f"No existing model found for {symbol}. Run full train first.")
            return False
            
        existing_model = joblib.load(model_file)
        existing_scaler = joblib.load(scaler_file)
        
        # Take only the recent data
        df_recent = df.tail(recent_n_days).copy()
        
        X, y = self.prepare_data(df_recent)
        if X is None or len(X) < 5:
            print(f"Not enough recent data to incrementally train {symbol}")
            return False
            
        # Scale using existing scaler
        X_scaled = existing_scaler.transform(X)
        
        # Train incrementally (using init_model)
        # Note: We don't use early stopping here as we just want to update with latest mini-batch
        existing_model.fit(
            X_scaled, y,
            init_model=existing_model
        )
        
        # Save updated model
        joblib.dump(existing_model, model_file)
        print(f"Incremental training completed and model updated for {symbol}")
        return True
