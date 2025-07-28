import pandas as pd
from datetime import datetime, timedelta
import os
import pickle
from yahooquery import Ticker
import requests
import csv
import time


def fetch_tickers_from_csv(cache_file):
    if os.path.exists(cache_file):
        with open(cache_file, newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # skip header
            tickers = [row[0].strip() for row in reader if row]
        print(f"✅ Loaded {len(tickers)} tickers from {cache_file}")
        return tickers
    else:
        print(f"❌ Cache file {cache_file} not found!")
        return []


def compute_dm_signals(df):
    close = df["close"].values
    length = len(close)
    if length < 20:
        return False, False, False, False

    TD = [0] * length
    TDUp = [0] * length
    TS = [0] * length
    TDDn = [0] * length

    for i in range(4, length):
        TD[i] = TD[i - 1] + 1 if close[i] > close[i - 4] else 0
        TS[i] = TS[i - 1] + 1 if close[i] < close[i - 4] else 0

    def valuewhen_reset(arr, idx):
        for j in range(idx - 1, 0, -1):
            if arr[j] < arr[j - 1]:
                return arr[j]
        return 0

    for i in range(4, length):
        TDUp[i] = TD[i] - valuewhen_reset(TD, i)
        TDDn[i] = TS[i] - valuewhen_reset(TS, i)

    DM9Top = TDUp[-1] == 9
    DM13Top = TDUp[-1] == 13
    DM9Bot = TDDn[-1] == 9
    DM13Bot = TDDn[-1] == 13

    return DM9Top, DM13Top, DM9Bot, DM13Bot


def load_or_fetch_price_data(tickers, interval, period, cache_key):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    cache_file = f"price_cache_{cache_key}_{today}.pkl"

    if os.path.exists(cache_file):
        print(f"📦 Using cached data: {cache_file}")
        with open(cache_file, "rb") as f:
            return pickle.load(f)

    print(f"🌐 Fetching fresh data for {cache_key}...")
    all_data = {}
    batch_size = 50

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        t = Ticker(batch)
        batch_data = t.history(interval=interval, period=period)

        if isinstance(batch_data, pd.DataFrame):
            for ticker in batch:
                if (ticker,) in batch_data.index:
                    all_data[ticker] = batch_data.xs(ticker, level=0)
        else:
            print(f"⚠️ Unexpected format in batch {batch}: {type(batch_data)}")

        time.sleep(1.5)

    with open(cache_file, "wb") as f:
        pickle.dump(all_data, f)

    return all_data


def scan_timeframe(tickers, interval_label, interval):
    results = {"Tops": [], "Bottoms": []}
    print(f"\n🔍 Scanning {len(tickers)} tickers on {interval_label} timeframe...")

    period = '2y' if interval == '1wk' else '6mo'
    price_data = load_or_fetch_price_data(tickers, interval, period, interval_label)

    for ticker, df in price_data.items():
        try:
            if df.empty:
                continue

            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]

            if interval == '1wk':
                last_date = df['date'].iloc[-1]
                if isinstance(last_date, datetime):
                    last_date = last_date.date()
            today = datetime.utcnow().date()
            if last_date >= today - timedelta(days=today.weekday()):
                df = df.iloc[:-1]

            DM9Top, DM13Top, DM9Bot, DM13Bot = compute_dm_signals(df)

            if DM9Top or DM13Top:
                signal = "DM13 Top" if DM13Top else "DM9 Top"
                results["Tops"].append((ticker, signal))

            if DM9Bot or DM13Bot:
                signal = "DM13 Bot" if DM13Bot else "DM9 Bot"
                results["Bottoms"].append((ticker, signal))

        except Exception as e:
            print(f"⚠️ Skipping {ticker} [{interval_label}] due to error: {e}")

    results["Tops"] = sorted(results["Tops"], key=lambda x: x[0])
    results["Bottoms"] = sorted(results["Bottoms"], key=lambda x: x[0])

    return results


def signals_to_html_table(signals):
    if not signals:
        return "<p>No signals.</p>"

    signals_sorted = sorted(signals, key=lambda x: x[0])
    html = "<table><tr><th>Ticker</th><th>Signal</th></tr>"
    for ticker, signal in signals_sorted:
        if signal == "DM9 Top":
            style = "background-color: #f8d7da;"
        elif signal == "DM13 Top":
            style = "background-color: #f5c6cb; font-weight: bold;"
        elif signal == "DM9 Bot":
            style = "background-color: #d4edda;"
        elif signal == "DM13 Bot":
            style = "background-color: #c3e6cb; font-weight: bold;"
        else:
            style = ""
        html += f"<tr><td>{ticker}</td><td style='{style}'>{signal}</td></tr>"
    html += "</table>"
    return html


def write_html_report(daily_results, weekly_results):
    html = f"""
    <html>
    <head>
        <meta charset=\"UTF-8\">
        <title>DeMark Signal Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1 {{ color: #333; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
            th, td {{ border: 1px solid #ccc; padding: 6px 8px; text-align: left; }}
            th {{ background-color: #f0f0f0; }}
        </style>
    </head>
    <body>
        <h1>🧭 DeMark Signal Report</h1>

        <h2>Daily Bottoms</h2>
        {signals_to_html_table(daily_results["Bottoms"])}

        <h2>Weekly Bottoms</h2>
        {signals_to_html_table(weekly_results["Bottoms"])}

        <h2>Daily Tops</h2>
        {signals_to_html_table(daily_results["Tops"])}

        <h2>Weekly Tops</h2>
        {signals_to_html_table(weekly_results["Tops"])}
    </body>
    </html>
    """

    os.makedirs("docs", exist_ok=True)
    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(html)


def main():
    print("⏳ Starting DeMark ASX Scanner")
    tickers = fetch_tickers_from_csv("asx_cache.csv")

    daily_results = scan_timeframe(tickers, "1D", "1d")
    weekly_results = scan_timeframe(tickers, "1W", "1wk")

    write_html_report(daily_results, weekly_results)
    print("✅ Report generated: docs/index.html")


if __name__ == "__main__":
    main()
