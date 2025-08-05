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
failed_log_path = '--'

# === DATE RANGE ===
end_date = datetime.today() - timedelta(days=3)
start_date = end_date - pd.DateOffset(years=8)

# === FOREX TICKERS ===
forex_pairs = [
    "EURUSD=X", "GBPUSD=X", "USDCHF=X", "USDJPY=X",
    "EURGBP=X", "EURCHF=X", "EURJPY=X",
    "GBPEUR=X", "GBPCHF=X", "GBPJPY=X",
    "AUDUSD=X", "NZDUSD=X", "USDCAD=X", "USDNOK=X", "USDSEK=X"
]

# === Download Loop ===
all_data = []
failed_pairs = []

for pair in forex_pairs:
    try:
        print(f"📥 Downloading {pair} ...")
        df = yf.Ticker(pair).history(start=start_date, end=end_date)
        if df.empty:
            raise ValueError("No data returned")
        df = df.reset_index()
        df['ticker'] = pair
        df = df[['Date', 'ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df.columns = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']
        all_data.append(df)
    except Exception as e:
        print(f"❌ Failed: {pair} — {e}")
        failed_pairs.append(pair)

# === Combine Data ===
final_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame(columns=['date','ticker','open','high','low','close','volume'])

# === PostgreSQL Connection ===
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# === Ensure Table Exists ===
with engine.begin() as conn:
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date DATE,
            ticker TEXT,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume BIGINT,
            PRIMARY KEY (date, ticker)
        );
    """))

# === Check Existing Data ===
try:
    existing = pd.read_sql(text(f"SELECT date, ticker FROM {table_name}"), engine)
except ProgrammingError:
    existing = pd.DataFrame(columns=["date", "ticker"])

# === Insert Only New Records ===
if not existing.empty and not final_df.empty:
    merged = final_df.merge(existing, on=["date", "ticker"], how="left", indicator=True)
    new_data = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
else:
    new_data = final_df.copy()

if not new_data.empty:
    print(f"📤 Inserting {len(new_data)} new FX rows …")
    new_data.to_sql(table_name, engine, if_exists='append', index=False, method='multi', chunksize=1000)
    print("✅ FX data inserted.")
else:
    print("✅ No new FX data to insert.")

# === Log Failed Tickers ===
if failed_pairs:
    with open(failed_log_path, "w") as f:
        for pair in failed_pairs:
            f.write(f"{pair}\n")
    print(f"⚠️ Logged {len(failed_pairs)} failed downloads.")
else:
    print("✅ All FX pairs processed.")
