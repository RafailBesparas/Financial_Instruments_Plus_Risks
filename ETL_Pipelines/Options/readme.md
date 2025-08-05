# 🧠 Options Data ETL Pipeline

This ETL pipeline fetches **options chain data** (calls and puts) for a curated list of major U.S. stocks using Yahoo Finance via `yfinance`, filters for contracts expiring within the next 30 days, and stores the result in a **PostgreSQL** table for further analysis or modeling.

---

## 📦 Key Features

- Fetches call and put options for ~50 large-cap U.S. equities
- Filters expiration dates to within the **next 30 days**
- Standardizes and appends options data to a PostgreSQL table
- Includes options Greeks and metadata
- Logs tickers with failed retrievals

---

## 💹 Sample Covered Stocks

| Sector        | Examples                          |
|---------------|-----------------------------------|
| Tech          | AAPL, MSFT, NVDA, AMD, META       |
| Finance       | JPM, BAC, GS, MS, SCHW            |
| Energy        | XOM, CVX, SLB, HAL                |
| Aerospace     | BA, GE, NOC, LMT                  |
| Pharma/Health | UNH, JNJ, LLY, PFE, MRK, ABBV     |
| Retail        | WMT, COST, TGT, MCD, DIS, HD      |

---

## 🧰 Tech Stack

- **Python 3.x**
- `pandas` – Data handling
- `yfinance` – Yahoo Finance API
- `SQLAlchemy` – PostgreSQL integration
- `psycopg2-binary` – PostgreSQL driver

---

## 🧾 Table Schema: `{table_name}`

| Column           | Type              | Description                        |
|------------------|-------------------|------------------------------------|
| underlying       | TEXT              | Stock ticker                       |
| expiration       | DATE              | Option expiration date             |
| contractSymbol   | TEXT              | Unique option symbol               |
| strike           | DOUBLE PRECISION  | Strike price                       |
| lastPrice        | DOUBLE PRECISION  | Last traded price                  |
| bid              | DOUBLE PRECISION  | Current bid                        |
| ask              | DOUBLE PRECISION  | Current ask                        |
| change           | DOUBLE PRECISION  | Price change since last close      |
| percentChange    | DOUBLE PRECISION  | Percent change                     |
| volume           | BIGINT            | Contracts traded today             |
| openInterest     | BIGINT            | Contracts open (not yet closed)    |
| impliedVolatility| DOUBLE PRECISION  | IV estimate (decimal, e.g., 0.43)  |
| inTheMoney       | BOOLEAN           | True if the option is in-the-money |
| type             | TEXT              | "call" or "put"                    |

---

## 📁 Project Structure

```bash
.
├── options_data_pipeline.py      # Main ETL script
├── failed_options.log            # Failed ticker log (auto-generated)
└── README.md                     # Documentation
