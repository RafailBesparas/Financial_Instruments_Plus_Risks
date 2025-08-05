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

# === COMMODITY FUTURES TICKERS (EU-RELEVANT) ===
commodities = [
    "BZ=F",  # Brent Crude Oil
    "NG=F",  # Natural Gas
    "HO=F",  # Heating Oil
    "GC=F",  # Gold
    "SI=F",  # Silver
    "HG=F",  # Copper
    "PL=F",  # Platinum
    "PA=F",  # Palladium
    "ZW=F",  # Wheat
    "ZC=F",  # Corn
    "ZS=F",  # Soybeans
    "KC=F",  # Coffee
    "CC=F",  # Cocoa
]

# === DATE RANGE ===
end_date = datetime.today() - timedelta(days=3)
start_date = end_date - pd.DateOffset(years=8)

# === DATA COLLECTION ===
all_data = []
failed = []

# Iterate throug each ticker in commodities with try and except
for ticker in commodities:
    try:
        # Give a message to check which ticker is being downloaded
        print(f" Downloading {ticker}...")
        # download the data
        df = yf.Ticker(ticker).history(start=start_date, end=end_date)

        # Check if the dataframe is empty
        if df.empty:
            raise ValueError("Empty dataframe")

        # Reset the index
        df = df.reset_index()
        # the ticker column will have the ticker object
        df['ticker'] = ticker
        # The columns of the dataframe would be these
        df = df[['Date', 'ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]
        # Rename the columns as here
        df.columns = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']
        # Append the data in the dataframe
        all_data.append(df)
    # Throw an exception if something wrong happens
    except Exception as e:
        # Throw a failed message for each ticker that failed
        print(f"Failed: {ticker}, reason: {e}")
        # In the failed dataframe add the ticker that failed
        failed.append(ticker)

# === COMBINE ALL DATA ===
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
else:
    final_df = pd.DataFrame(columns=['date', 'ticker', 'open', 'high', 'low', 'close', 'volume'])

# === CONNECT TO DATABASE ===
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# === CREATE TABLE IF NOT EXISTS ===
with engine.connect() as conn:
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date DATE,
            ticker TEXT,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume BIGINT
        );
    """))

    # Fetch existing keys
    try:
        # Fetch the data needed 
        existing = pd.read_sql(text(f"SELECT date, ticker FROM {table_name}"), conn)
    except ProgrammingError:
        existing = pd.DataFrame(columns=["date", "ticker"]) 

# === DEDUPLICATION LOGIC ===
# Check if both the existing data from the database and the newly collected data are non-empty
if not existing.empty and not final_df.empty:
    # Merge the new data (final_df) with the existing data on 'date' and 'ticker' columns
    # using a left join and add an indicator column to track the source of each row
    merged = final_df.merge(existing, on=["date", "ticker"], how="left", indicator=True)
    # Filter only the rows that are present *only* in the new data (left_only)
    # These are the rows that do not exist in the database yet
    new_data = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
else:
    # If there is no existing data, or the new data is empty, skip the merge and use the new data as-is
    new_data = final_df.copy()

# === INSERT NEW DATA ===
if not new_data.empty:
    print(f" Inserting {len(new_data)} new rows into '{table_name}'...")
    new_data.to_sql(table_name, engine, if_exists="append", index=False)
    print("Insert complete.")
else:
    print("No new commodity data to insert.")

# === LOG FAILURES ===
if failed:
    with open(failed_log_path, "w") as f:
        for ticker in failed:
            f.write(f"{ticker}\n")
    print(f"Failed tickers logged to '{failed_log_path}'")
else:
    print("All tickers successfully processed.")
