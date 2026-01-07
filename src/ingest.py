import os
from datetime import datetime, timezone

import duckdb
import pandas as pd
import yfinance as yf

TICKERS = ["SPY", "QQQ", "DIA", "IWM", "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"]
DB_PATH = "data/db/marketpulse.duckdb"
PARQUET_PATH = "data/raw/prices.parquet"


def ensure_dirs() -> None:
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/db", exist_ok=True)


def download_prices(tickers, period="1y", interval="1d"):
    frames = []

    for t in tickers:
        print(f"Downloading {t}...")

        df = yf.Ticker(t).history(
            period=period,
            interval=interval,
            auto_adjust=False
        )

        if df is None or df.empty:
            print(f"❌ No data for {t}")
            continue

        df = df.reset_index()

        df["ticker"] = t
        df = df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        })

        df["date"] = pd.to_datetime(df["date"]).dt.date

        frames.append(df[[
            "date", "ticker",
            "open", "high", "low",
            "close", "adj_close", "volume"
        ]])

        print(f"✅ {t}: {len(df)} rows")

    if not frames:
        print("❌ No data downloaded at all")
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    return out



def write_duckdb(df: pd.DataFrame) -> None:
    con = duckdb.connect(DB_PATH)

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_prices (
            date DATE,
            ticker VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            adj_close DOUBLE,
            volume DOUBLE,
            ingested_at TIMESTAMP
        )
    """)

    df2 = df.copy()
    df2["ingested_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

    # create a temp table instead of relying on IN with tuples
    con.register("incoming_df", df2)
    con.execute("CREATE TEMP TABLE incoming AS SELECT * FROM incoming_df")

    # delete matching keys using JOIN (supported)
    con.execute("""
        DELETE FROM raw_prices r
        USING incoming i
        WHERE r.ticker = i.ticker
          AND r.date = i.date
    """)

    # insert new rows
    con.execute("INSERT INTO raw_prices SELECT * FROM incoming")

    con.close()



def main() -> None:
    ensure_dirs()
    df = download_prices(TICKERS)
    df.to_parquet(PARQUET_PATH, index=False)
    write_duckdb(df)

    print(f"Downloaded rows: {len(df):,}")
    print(f"Saved parquet: {PARQUET_PATH}")
    print(f"Updated DuckDB: {DB_PATH}")


if __name__ == "__main__":
    main()
