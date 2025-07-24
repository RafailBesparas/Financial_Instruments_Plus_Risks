import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

# Database Configuration
db_user='----'; db_pass='-----'; db_host='---'; db_port='---'; db_name='----'
table='bond_data'; failed_log='failed_bonds.log'

# Date range (last 8 years)
end_date = datetime.today().date() - timedelta(days=3) # end date is the previous three days from today (if we have today 22 then my end date will be 19)
start_date = end_date - pd.DateOffset(years=8) # my start date will check my end date and it will go 8 yests back

bond_tickers = [
    # European Sovereign & Corporate Bond ETFs
    'IEGA.L',  # iShares Core Euro Corporate Bond UCITS
    'IGLT.L',  # iShares Core UK Gilts UCITS
    'EMIM.L',  # iShares Core Global Aggregate Bond UCITS
    'EUNA.L',  # iShares EUR Corporate Bond UCITS
    'IBGL.L',  # iShares € Govt Bond 15-30yr UCITS
    'IHRD.L',  # iShares $ Corp Bond UCITS
    'IBTM.L',  # iShares $ Treasuries 7-10yr UCITS
    'LUAG.DE', # Xtrackers II EUR Corporate Bond UCITS

    # Global Bond ETFs
    'BND',     # Vanguard Total Bond Market ETF
    'AGG',     # iShares Core US Aggregate Bond
    'TLT',     # iShares 20+ Year Treasury Bond
    'LQD',     # iShares iBoxx $ Investment Grade Corp
    'IEF',     # iShares 7-10 Year Treasury Bond

    # Eurozone Bond Futures
    'EUX1.DE', # Euro Bund Futures proxy (Xetra)
    'EUX2.DE', # Euro Buxl (Long) Futures proxy
]

# the first list will contain all the data and the second will contain the failed bonds
all_data=[]; failed=[]

# -------------------------------------- Initialize the dataframes ---------------------------------------------

# check all the tickers inside the bond tickets
for ticker in bond_tickers:
    try:
        print(f"Downloading {ticker} …")
        df = yf.Ticker(ticker).history(start=start_date, end=end_date) # start downloading the ticker from yahoo finance
        # if the dataframe is empty throw error No data
        if df.empty:
            raise ValueError("No data")
        # Flatten the dataframe 
        df = df.reset_index()
        # Assign to the index column the index data
        df['ticker']=ticker
        # Select the folowing columns
        df=df[['Date','ticker','Open','High','Low','Close','Volume']]
        # rename the columns for better clarification and also standardize them and make them lower case
        df.columns=['date','ticker','open','high','low','close','volume']
        # append everything to the dataframe
        all_data.append(df)
    # If something does not work throw and error
    except Exception as e:
        # Throw an error for each failed ticker
        print(f" Failed {ticker}: {e}")
        failed.append(ticker) # every ricker will go to the failed list to be printed in the end

# It combines all dataframes in all_data into one; if all_data is empty, it creates an empty dataframe with the expected columns
final_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame(columns=['date','ticker','open','high','low','close','volume'])

# --------------------------------------- Database Engine--------------------------------------------------------------

# Start to query the databse and first job initialize the enginer
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# Create table Stocks
with engine.begin() as conn:
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {table} (
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

# ---------------------------------------------------------- Checks Existing Data - Add New Data -------------------------

# Check the existing data in the databse
try:
    # get all the data from the database
    existing = pd.read_sql(text(f"SELECT date,ticker FROM {table}"), engine)
except ProgrammingError: # check for an error
    existing = pd.DataFrame(columns=['date','ticker']) # Set to the existing the data from the database

# Append only new data
if not existing.empty and not final_df.empty:
    # merge the existing data df with the new data
    merged = final_df.merge(existing, on=['date','ticker'], how='left', indicator=True)
    new_data = merged[merged['_merge']=='left_only'].drop(columns=['_merge'])
else:
    new_data = final_df.copy() # pass to the new data the final df

# If there the new data dataframe is not empty
if not new_data.empty:
    print(f" Inserting {len(new_data)} rows …")
    new_data.to_sql(table, engine, if_exists='append', index=False, method='multi', chunksize=1000) # append the new data to the database
else:
    print("No new records added.")

# Log failures
if failed:
    with open(failed_log, 'w') as f:
        f.write('\n'.join(failed))
    print(f"⚠️ Failed tickers logged in {failed_log}")
else:
    print("✅ All tickers processed.")
