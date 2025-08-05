# 🏦 Fund Data ETL Pipeline (Top 100 European Funds)

This Python ETL pipeline fetches **historical OHLCV data** for the **top 100 exchange-traded funds (ETFs)** and mutual funds in Europe using `yfinance`, transforms it into a structured format, and loads it into a **PostgreSQL** database.

It includes features like deduplication, automatic table creation, failure logging, and scalable batch inserts.

---

## ✅ Key Features

- 🗃️ Tracks over 100 top European funds by ticker
- 📆 Automatically retrieves 8 years of historical daily data
- 🔄 Skips duplicates using `(date, ticker)` primary key
- 🧾 Logs any failed downloads for troubleshooting
- ⚙️ Uses batch inserts (`chunksize=1000`) for scalability

---

## 💹 Fund Coverage

Sample of tracked funds:

| Ticker   | Description (Example)         |
|----------|-------------------------------|
| IMEU.DE  | iShares MSCI EMU ETF          |
| SWDA.L   | iShares Core MSCI World       |
| VUSA.L   | Vanguard S&P 500 UCITS ETF    |
| IWDA.AS  | iShares Core MSCI World       |
| XDWD.DE  | Xtrackers MSCI World UCITS    |
| ...      | ~100 total tickers             |

---

## 🧰 Tech Stack

- **Python 3.x**
- [`yfinance`](https://pypi.org/project/yfinance/)
- `pandas`
- `SQLAlchemy` (ORM)
- `PostgreSQL`
- `psycopg2-binary` (DB driver)

---

## 📁 Project Structure

```bash
.
├── fund_data_to_database.py    # Main ETL script
├── failed_funds.log            # Log of failed downloads (generated at runtime)
└── README.md                   # Project documentation
