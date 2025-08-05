import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

# === CONFIGURATION ===
db_user = '--'
db_pass = '--'
db_host = '--'
db_port = '--'
db_name = '--'
table_name = '--'
etf_csv_path = '--'  # Or replace with your list
failed_log_path = '--'

# === DATE RANGE ===
end_date = datetime.today() - timedelta(days=3)
start_date = end_date - pd.DateOffset(years=8)

# === Load ETF List ===
etf_df = pd.read_csv(etf_csv_path)
etf_list = etf_df['Symbol'].dropna().unique().tolist()

# === Collect ETF Data ===
all_data = []
failed = []

for ticker in etf_list:
    try:
        print(f"Downloading {ticker}...")
        df = yf.Ticker(ticker).history(start=start_date, end=end_date)
        if df.empty:
            raise ValueError("Empty DataFrame")

        df = df.reset_index()
        df['ticker'] = ticker
        df = df[['Date', 'ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df.columns = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']
        all_data.append(df)

    except Exception as e:
        print(f"Failed: {ticker}, reason: {e}")
        failed.append(ticker)

# === Combine Data ===
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
else:
    final_df = pd.DataFrame(columns=['date', 'ticker', 'open', 'high', 'low', 'close', 'volume'])

# === PostgreSQL Connection ===
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# === Ensure Table Exists & Read Existing Records ===
with engine.connect() as conn:
    print(f"📌 Ensuring table '{table_name}' exists...")
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date DATE,
            ticker TEXT,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume BIGINT,
            CONSTRAINT idx_etfs_date_ticker UNIQUE (date, ticker)
        );
    """))

    try:
        print("Reading existing ETF records...")
        existing = pd.read_sql(text(f"SELECT date, ticker FROM {table_name}"), conn)
    except ProgrammingError:
        print("Table not readable yet. Starting fresh.")
        existing = pd.DataFrame(columns=['date', 'ticker'])

# === Filter New Records Only ===
if not existing.empty and not final_df.empty:
    merged = final_df.merge(existing, on=["date", "ticker"], how="left", indicator=True)
    new_data = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
else:
    new_data = final_df.copy()

# === Insert New Data ===
if not new_data.empty:
    print(f"Inserting {len(new_data)} new rows into '{table_name}'...")
    new_data.to_sql(table_name, engine, if_exists="append", index=False)
    print("Insert complete.")
else:
    print("No new ETF data to insert.")

# === Log Failed Tickers ===
if failed:
    with open(failed_log_path, "w") as f:
        for ticker in failed:
            f.write(f"{ticker}\n")
    print(f"Failed tickers logged to '{failed_log_path}'")
else:
    print("All tickers processed successfully.")
