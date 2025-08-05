import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, OperationalError

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

# === TICKER LIST (Top 100 Funds in Europe by Ticker)
fund_tickers = [
    'IMEU.DE', 'SWDA.L', 'VUSA.L', 'VWRL.AS', 'IWDA.AS', 'ISF.L', 'EQQQ.DE', 'EXXT.DE', 'SXR8.DE', 'XDWD.DE',
    'VWRL.L', 'CSNDX.SW', 'CSPX.L', 'IUSA.L', 'IEMG', 'IWRD.L', 'IWVL.AS', 'SUAS.AS', 'XESC.DE', 'ISAC.AS',
    'VEVE.L', 'SXRW.DE', 'SMT.L', 'VERX.DE', 'XDUK.DE', 'CSBGC.SW', 'LU0635178014', 'ISJP.L', 'EMIM.L', 'IEEM.L',
    'XD9U.DE', 'EXX7.DE', 'IEUX.L', 'VEUR.L', 'SPYW.DE', 'SXR1.DE', 'EIMI.L', 'CSP1.L', 'IQQQ.DE', 'XMME.DE',
    'ESGE.AS', '2B76.DE', 'IITU.L', 'XDUK.L', 'SSEW.L', 'IAGG', 'XUKE.L', 'IE00B3XXRP09', 'XDJP.DE', 'IWFM.L',
    'SUSW.L', 'VETY.L', 'SGWD.L', 'XDWT.DE', 'IE00B42NKQ00', 'VHVG.DE', 'XBAE.DE', 'SDJH.DE', 'IE00B1XNHC34',
    'V3AA.DE', 'XD9E.DE', 'IUSQ.DE', 'XD7T.DE', 'XMMT.DE', 'SWRA.L', 'EMIM.DE', 'V3AE.DE', 'XDW0.DE', 'VDWR.DE',
    'ESPO.L', 'XDWT.L', 'VHYG.DE', 'IWVL.L', 'ISPY.L', 'IUIT.L', 'EUNA.L', 'XDWT.AS', 'XDWQ.DE', 'CSNDX.L',
    'EXXY.DE', 'EUFN', 'SXR7.DE', 'VWCE.DE', 'CSP1.AS', 'ISPA.DE', 'EXXT.L', 'XEMD.DE', 'IMEU.L', 'XD9U.L',
    'XDWD.L', 'SPYG.L', 'IWRD.AS', 'XDW0.AS', 'IUSQ.L', 'VEUR.DE', 'VEVE.DE', 'ISF.DE', 'VHVG.L'
]

# === Download Loop ===
all_data = []
failed_tickers = []

for ticker in fund_tickers:
    try:
        print(f"📥 Downloading {ticker}...")
        df = yf.Ticker(ticker).history(start=start_date, end=end_date)
        if df.empty:
            raise ValueError("No data returned")
        df = df.reset_index()
        df['ticker'] = ticker
        df = df[['Date', 'ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df.columns = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']
        all_data.append(df)
    except Exception as e:
        print(f"❌ Failed to download {ticker}: {e}")
        failed_tickers.append(ticker)

# === Combine All Data ===
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
else:
    final_df = pd.DataFrame(columns=['date', 'ticker', 'open', 'high', 'low', 'close', 'volume'])

# === PostgreSQL Connection ===
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# === Ensure Table Exists ===
with engine.begin() as conn:
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
            PRIMARY KEY (date, ticker)
        );
    """))

# === Fetch Existing Records ===
try:
    with engine.connect() as conn:
        existing = pd.read_sql(text(f"SELECT date, ticker FROM {table_name}"), conn)
except ProgrammingError:
    print("⚠️ Failed to check existing data. Assuming table is empty.")
    existing = pd.DataFrame(columns=["date", "ticker"])

# === Remove Duplicates ===
if not existing.empty and not final_df.empty:
    merged = final_df.merge(existing, on=["date", "ticker"], how="left", indicator=True)
    new_data = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
else:
    new_data = final_df.copy()

# === Insert New Data ===
if not new_data.empty:
    print(f"📤 Inserting {len(new_data)} new rows into '{table_name}'...")
    new_data.to_sql(table_name, engine, if_exists="append", index=False, method="multi", chunksize=1000)
    print("✅ Insert complete.")
else:
    print("✅ No new fund data to insert.")

# === Log Failed Tickers ===
if failed_tickers:
    with open(failed_log_path, "w") as f:
        for ticker in failed_tickers:
            f.write(f"{ticker}\n")
    print(f"⚠️ Failed tickers logged to '{failed_log_path}'")
else:
    print("✅ All tickers successfully processed.")
