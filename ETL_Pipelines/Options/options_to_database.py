import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# === CONFIGURATION ===
db_user = '--'
db_pass = '--'
db_host = '--'
db_port = '--'
db_name = '--'
table_name = '--'

# === TICKERS ===
tickers = [
    'AAPL', 'MSFT', 'NVDA', 'AMD', 'TSLA', 'META', 'AMZN', 'GOOG', 'AVGO', 'CRM',
    'JPM', 'BAC', 'GS', 'MS', 'V', 'MA', 'PYPL', 'SQ', 'COIN', 'SCHW',
    'XOM', 'CVX', 'SLB', 'HAL', 'BA', 'GE', 'NOC', 'LMT', 'CAT', 'DE',
    'UNH', 'JNJ', 'LLY', 'PFE', 'MRK', 'AMGN', 'REGN', 'BMY', 'CVS', 'ABBV',
    'WMT', 'COST', 'TGT', 'MCD', 'HD', 'DIS', 'SBUX', 'NKE', 'BKNG', 'ROKU'
]

# === DATE RANGE ===
today = datetime.today().date()
future = today + timedelta(days=30)

# === Connect to PostgreSQL ===
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# === Ensure Table Exists ===
with engine.connect() as conn:
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            underlying TEXT,
            expiration DATE,
            contractSymbol TEXT,
            strike DOUBLE PRECISION,
            lastPrice DOUBLE PRECISION,
            bid DOUBLE PRECISION,
            ask DOUBLE PRECISION,
            change DOUBLE PRECISION,
            percentChange DOUBLE PRECISION,
            volume BIGINT,
            openInterest BIGINT,
            impliedVolatility DOUBLE PRECISION,
            inTheMoney BOOLEAN,
            type TEXT
        );
    """))

# === Download Options Data ===
all_data = []
failed = []

for ticker in tickers:
    try:
        print(f"📥 Fetching options for {ticker}")
        tk = yf.Ticker(ticker)
        for exp in tk.options:
            exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
            if today < exp_date <= future:
                calls = tk.option_chain(exp).calls
                puts = tk.option_chain(exp).puts

                calls["type"] = "call"
                puts["type"] = "put"

                options_df = pd.concat([calls, puts], ignore_index=True)
                options_df["underlying"] = ticker
                options_df["expiration"] = exp_date
                all_data.append(options_df)
    except Exception as e:
        print(f"❌ Failed for {ticker}: {e}")
        failed.append(ticker)

# === Save to Database ===
if all_data:
    df = pd.concat(all_data, ignore_index=True)
    df = df[['underlying', 'expiration', 'contractSymbol', 'strike', 'lastPrice', 'bid',
             'ask', 'change', 'percentChange', 'volume', 'openInterest', 'impliedVolatility',
             'inTheMoney', 'type']]
    
    print(f"📤 Inserting {len(df)} options records...")
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print("✅ Insert complete.")
else:
    print("⚠️ No options data collected.")

# === Log Failures ===
if failed:
    with open("failed_options.log", "w") as f:
        for item in failed:
            f.write(item + "\n")
    print("⚠️ Failed tickers logged.")
