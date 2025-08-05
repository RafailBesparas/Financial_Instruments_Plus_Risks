# 💱 Forex Historical Data ETL Pipeline

A robust Python-based ETL pipeline that downloads 8 years of **historical FX (foreign exchange) data** from Yahoo Finance, transforms it into a clean format, and loads it into a **PostgreSQL** database. It includes deduplication, table creation, and failure logging.

---

## 📈 Supported Currency Pairs

This pipeline collects OHLCV data for the following major and cross FX pairs:

| Symbol     | Description          |
|------------|----------------------|
| EURUSD=X   | Euro / US Dollar     |
| GBPUSD=X   | British Pound / USD  |
| USDCHF=X   | US Dollar / Swiss Franc |
| USDJPY=X   | US Dollar / Japanese Yen |
| EURGBP=X   | Euro / British Pound |
| EURCHF=X   | Euro / Swiss Franc   |
| EURJPY=X   | Euro / Japanese Yen  |
| GBPEUR=X   | British Pound / Euro |
| GBPCHF=X   | British Pound / Swiss Franc |
| GBPJPY=X   | British Pound / Japanese Yen |
| AUDUSD=X   | Australian Dollar / USD |
| NZDUSD=X   | New Zealand Dollar / USD |
| USDCAD=X   | US Dollar / Canadian Dollar |
| USDNOK=X   | US Dollar / Norwegian Krone |
| USDSEK=X   | US Dollar / Swedish Krona  |

---

## 🛠️ Technologies Used

- **Python 3**
- [`yfinance`](https://pypi.org/project/yfinance/) for financial data
- `pandas` for data manipulation
- `SQLAlchemy` for PostgreSQL integration
- `psycopg2-binary` as the PostgreSQL driver

---

## 📦 Table Schema: `forex_data`

| Column  | Type              | Description         |
|---------|-------------------|---------------------|
| date    | `DATE`            | Trading date        |
| ticker  | `TEXT`            | Forex symbol        |
| open    | `DOUBLE PRECISION`| Opening price       |
| high    | `DOUBLE PRECISION`| Highest price       |
| low     | `DOUBLE PRECISION`| Lowest price        |
| close   | `DOUBLE PRECISION`| Closing price       |
| volume  | `BIGINT`          | Volume traded       |

> A `PRIMARY KEY (date, ticker)` constraint is used to ensure uniqueness.

---

## 📂 Project Structure

```bash
.
├── forex_to_database.py        # Main ETL script
├── failed_fx_pairs.log         # Log file for failed downloads (generated at runtime)
└── README.md                   # Documentation
