# Project Description
- This Python project is a bond data ETL (Extract, Transform, Load) pipeline that fetches historical bond ETF and futures data from Yahoo Finance for the past 8 years and stores it in a PostgreSQL database. Here's what it does:
1. Defines a list of bond tickers, including European, Global, and Eurozone bond ETFs and futures.
2. Downloads historical price data (Open, High, Low, Close, Volume) using the yfinance library.
3. Standardizes and consolidates the data into a single DataFrame.
4. Creates a PostgreSQL table (bond_data) if it doesn't exist.
5. Checks for existing records in the database and inserts only new ones.
6. Logs failed ticker downloads for review.

# What Is a Bond?
- A bond is a fixed-income instrument that represents a loan made by an investor to a borrower (typically a government or corporation). It includes:
- Principal (face value): Amount to be repaid at maturity.
- Coupon: Periodic interest payments to the bondholder.
- Maturity Date: The date the bond’s principal is repaid.
- Bonds are used to raise capital and are generally considered lower-risk than stocks.

# List of Bonds
| Ticker  | Description                              |
| ------- | ---------------------------------------- |
| IEGA.L  | iShares Core Euro Corporate Bond UCITS   |
| IGLT.L  | iShares Core UK Gilts UCITS              |
| EMIM.L  | iShares Core Global Aggregate Bond UCITS |
| EUNA.L  | iShares EUR Corporate Bond UCITS         |
| IBGL.L  | iShares € Govt Bond 15-30yr UCITS        |
| IHRD.L  | iShares \$ Corp Bond UCITS               |
| IBTM.L  | iShares \$ Treasuries 7-10yr UCITS       |
| LUAG.DE | Xtrackers II EUR Corporate Bond UCITS    |
| BND     | Vanguard Total Bond Market ETF           |
| AGG     | iShares Core US Aggregate Bond           |
| TLT     | iShares 20+ Year Treasury Bond           |
| LQD     | iShares iBoxx \$ Investment Grade Corp   |
| IEF     | iShares 7-10 Year Treasury Bond          |
| EUX1.DE | Euro Bund Futures proxy (Xetra)          |
| EUX2.DE | Euro Buxl (Long) Futures proxy           |
