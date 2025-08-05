# 🛢️ Commodities ETL Pipeline

A Python-based ETL pipeline that fetches historical commodity futures data (oil, metals, agriculture) from Yahoo Finance using `yfinance`, transforms the data into a standardized format, and loads it into a PostgreSQL database. The pipeline is optimized for EU-relevant tickers and includes deduplication, failure logging, and robust database integration using SQLAlchemy.

---

## 📦 Features

- Downloads historical OHLCV data for 13 major commodity futures.
- Dynamically calculates the date range (default: last 8 years, ending 3 days ago).
- Cleans and structures the data with relevant financial columns.
- Checks for duplicates before inserting into the database.
- Logs any failed downloads for further inspection.
- Automatically creates the PostgreSQL table if it doesn't exist.

---

## 📈 Target Commodities

The following commodity futures (from Yahoo Finance) are included:

| Ticker | Commodity        |
|--------|------------------|
| BZ=F   | Brent Crude Oil  |
| NG=F   | Natural Gas       |
| HO=F   | Heating Oil       |
| GC=F   | Gold              |
| SI=F   | Silver            |
| HG=F   | Copper            |
| PL=F   | Platinum          |
| PA=F   | Palladium         |
| ZW=F   | Wheat             |
| ZC=F   | Corn              |
| ZS=F   | Soybeans          |
| KC=F   | Coffee            |
| CC=F   | Cocoa             |

---

## 🧰 Tech Stack

- **Python** 🐍
- **pandas** – Data manipulation
- **yfinance** – Yahoo Finance API
- **SQLAlchemy** – Database ORM
- **PostgreSQL** – Target database

---