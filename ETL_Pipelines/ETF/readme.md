# 📊 ETF Historical Data ETL Pipeline

This is a Python-based ETL pipeline that retrieves **historical data for a list of Exchange-Traded Funds (ETFs)** from Yahoo Finance, cleans and formats the data, and loads it into a **PostgreSQL database**. The pipeline automatically handles deduplication and logs any failed downloads for review.

---

## 🚀 Features

- 📥 **Automatic download** of historical ETF data (8 years back)
- 🧹 **Data transformation** into clean OHLCV format
- 🗃️ **PostgreSQL integration** with table creation and deduplication
- 📄 **Error logging** for any failed tickers
- 🔄 **Incremental loads** – skips already existing data

---

## 🧾 How It Works

1. Reads a list of ETF tickers from a CSV file (expects a `Symbol` column).
2. Downloads OHLCV data for each ticker using `yfinance`.
3. Formats and cleans the data.
4. Creates the PostgreSQL table if it doesn’t exist.
5. Checks for duplicates based on `(date, ticker)`.
6. Inserts only new records.
7. Logs any tickers that failed during download.

---

## 📁 Project Structure

```bash
.
├── etf_data_pipeline.py         # Main ETL script
├── etfs.csv                     # Input file with list of ETF tickers
├── failed_etfs.txt              # (Generated) List of failed downloads
└── README.md                    # Project documentation
