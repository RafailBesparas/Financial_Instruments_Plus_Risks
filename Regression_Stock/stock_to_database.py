import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# === CONFIGURATION ===
db_user = '----'
db_pass = '---'
db_host = '---'
db_port = '---'
db_name = '---'
table_name = '---'

# === Setup Date Range ===
end_date = datetime.today() - timedelta(days=3) # end date is the previous three days from today (if we have today 22 then my end date will be 19)
start_date = end_date - pd.DateOffset(years=8) # my start date will check my end date and it will go 8 yests back

# === Load Symbols from Wikipedia===
# Read the HTML page 
sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
# Get the symbols and replace the . with -
sp500['Symbol'] = sp500['Symbol'].str.replace('.', '-', regex=False)
# Keep only the unique symbols to list
symbols_list = sp500['Symbol'].unique().tolist()

# === Download OHLCV Data ===
print("---- Downloading data from Yahoo Finance...")
raw_data = yf.download(
    tickers=symbols_list,     # List of stock ticker symbols (e.g., ['AAPL', 'GOOG']) to fetch data for
    start=start_date,         # Start date for historical data (e.g., '2022-01-01')
    end=end_date,             # End date for historical data (e.g., '2023-01-01')
    group_by='ticker',        # Organize the data by ticker symbol, creating a hierarchical column structure
    threads=True,             # Use multiple threads for faster data download (especially with many tickers)
    progress=False,           # Suppress progress bar during download
    auto_adjust=False         # Do not adjust prices for dividends or stock splits
)

# === Filter & Clean a little bit the data ====

# Drop all the nan values
raw_data = raw_data.dropna(how='all', axis=1)

# Reshape the multi-level column DataFrame into a standard 2D DataFrame
df = raw_data.stack(future_stack=True)

# Rename the index levels of the stacked DataFrame for clarity
df.index.names = ['date', 'ticker']

# make the column text (headers) lowercase 
df.columns = df.columns.str.lower()

# Ensure expected columns exist
expected_cols = ['open', 'high', 'low', 'close', 'volume']

# Check if there is missing column in the expected columns
missing_cols = [col for col in expected_cols if col not in df.columns]

# Add any missing columns (from expected_cols) to the DataFrame and fill them with NA values
for col in missing_cols:
    df[col] = pd.NA # Ensures the DataFrame has all required columns, even if some are missing from the raw data
# Reorder the DataFrame columns to match the expected structure
df = df[expected_cols]
# Reset the index so that 'date' and 'ticker' become regular columns instead of index levels
df = df.reset_index()

# === Connect to PostgreSQL Database ===

# Create the standard engine for the database
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# connect to the database and check the existing records
with engine.connect() as connection:
    print(" Checking for existing records...")
    
    # Fetch existing dates and tickers, query the database to get the existing tickers
    result = connection.execute(text(f"SELECT DISTINCT date, ticker FROM {table_name}"))

    # Make the existing tickers a dataframe, fetching all the records
    existing = pd.DataFrame(result.fetchall(), columns=['date', 'ticker'])
    
    # Merge and remove duplicates
    # Perform a left merge to identify new rows in 'df' that are not yet in 'existing'
    merged = df.merge(existing, on=['date', 'ticker'], how='left', indicator=True)
    
    # Filter only the rows that are present in 'df' but not in 'existing'
    # These are the new records that need to be processed or inserted
    new_data = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge']) # Clean up by dropping the merge indicator

    # Check if the new data is not empty
    if not new_data.empty:
        print(f" Inserting {len(new_data)} new rows into '{table_name}'...") # Message
        new_data.to_sql(table_name, engine, if_exists='append', index=False) # Insert the new data to the database
        print("Insert complete.") # Completed the insert
    else: # when there is no new data
        print("No new data to insert. All records already exist.") # Print this message
