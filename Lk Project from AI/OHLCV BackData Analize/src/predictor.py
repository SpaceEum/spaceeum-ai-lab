import os
import joblib
import pandas as pd
import numpy as np

class Predictor:
    def __init__(self, model_dir: str = "./models"):
        self.model_dir = model_dir
        
    def predict_next_day(self, df: pd.DataFrame, symbol: str) -> dict:
        """
        Loads the saved model and scaler, processes the latest data point,
        and predicts the next day's Close price.
        """
        model_file = os.path.join(self.model_dir, f"{symbol}_lgb.pkl")
        scaler_file = os.path.join(self.model_dir, f"{symbol}_scaler.pkl")
        
        if not os.path.exists(model_file) or not os.path.exists(scaler_file):
            return {"error": "Model files not found"}
            
        model = joblib.load(model_file)
        scaler = joblib.load(scaler_file)
        
        # Get the VERY LAST row of data (today) to predict tomorrow
        latest_row = df.iloc[-1:].copy()
        current_close = latest_row['Close'].values[0]
        current_date = latest_row['Open_Time'].values[0]
        
        # Prepare features exactly as the trainer did
        drop_cols = ['Open_Time', 'Target_Return']
        available_drop_cols = [col for col in drop_cols if col in latest_row.columns]
        X_latest = latest_row.drop(columns=available_drop_cols)
        
        for col in X_latest.columns:
            if 'Time' in col or X_latest[col].dtype == 'datetime64[ns]':
                X_latest = X_latest.drop(columns=[col])
                
        # Fill any NaNs
        X_latest = X_latest.fillna(0) # or median if we saved medians, but 0 is safe for tail
        
        # Scale and Predict Log Return
        X_latest_scaled = scaler.transform(X_latest)
        pred_log_return = model.predict(X_latest_scaled)[0]
        
        # Convert Log Return back to Expected Close Price
        # Log_Return = ln(P_t1 / P_t0) -> P_t1 = P_t0 * exp(Log_Return)
        pred_price = current_close * np.exp(pred_log_return)
        
        return {
            "symbol": symbol,
            "last_date": pd.to_datetime(current_date),
            "current_close": current_close,
            "predicted_return_pct": (np.exp(pred_log_return) - 1) * 100,
            "predicted_close": pred_price
        }
