# Stock Price Prediction – Regression Modeling
- This project predicts the next-day closing price of S&P 500 stocks using historical OHLCV data and engineered financial features.

# predict_next_day_closing_price.ipynb
- Objective: Predict next_day_close using past OHLCV data and derived signals.
- Target: next_day_close = close.shift(-1)
- Key Features:
1. Price stats: open, high, low, volume
2. Rolling indicators: moving averages, volatility
3. Calendar signals: day_of_week, is_month_end
4. Momentum: returns, log returns, spreads

- Models Tested:
1. Linear, Ridge, Lasso, ElasticNet
2. Random Forest (best), Decision Tree, XGBoost
3. Best Result: Random Forest with R² = 0.9781, RMSE = 66.06

# Stock_Feature_Engineering.ipynb
- Objective: Explore and build financial features from OHLCV data.
- Content:
1. Dataset overview and target definition
2. Feature creation: moving averages, momentum, volatility
3. Model comparison for regression performance

# Outcome:
- Identified best-performing models
- Highlighted need for time-aware validation and better features

# Limitations & Next Steps
- No time-series cross-validation used (currently random split)
- Linear models lack feature scaling
- XGBoost and tree models need hyperparameter tuning
- Future: add technical indicators (RSI, MACD), use SHAP for explainability