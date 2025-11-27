import sqlite3
import pandas as pd
import random
import matplotlib.pyplot as plt
import streamlit as st

# -------------------------------
# 1️⃣ DATABASE + DATA GENERATION
# -------------------------------

# Connect to SQLite and create table
conn = sqlite3.connect('trading.db')
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS trades (
    trade_id INTEGER PRIMARY KEY,
    user_id INTEGER,
    stock_symbol TEXT,
    transaction_type TEXT,
    price REAL,
    quantity INTEGER,
    trade_date TEXT
)
''')

# Check if data already exists
cur.execute("SELECT COUNT(*) FROM trades")
count = cur.fetchone()[0]

if count == 0:  # Insert only once
    stocks = ['AAPL', 'TSLA', 'GOOGL', 'AMZN', 'MSFT']
    data = []

    for i in range(1, 1001):
        data.append((
            i,
            random.randint(1, 50),
            random.choice(stocks),
            random.choice(['BUY', 'SELL']),
            round(random.uniform(100, 1500), 2),
            random.randint(1, 100),
            f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        ))

    cur.executemany('''
    INSERT INTO trades (trade_id, user_id, stock_symbol, transaction_type, price, quantity, trade_date)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', data)
    conn.commit()

conn.close()


# -------------------------------
# 2️⃣ EXTRACT & TRANSFORM
# -------------------------------

conn = sqlite3.connect('trading.db')
df = pd.read_sql('SELECT * FROM trades', conn)
conn.close()

df['total_value'] = df['price'] * df['quantity']

# Compute stock summary for later use
stock_summary = df.groupby('stock_symbol')['total_value'].sum().reset_index()

# Save transformations to another DB
conn_new = sqlite3.connect('trading_summary.db')
stock_summary.to_sql('stock_total_value', conn_new, if_exists='replace', index=False)
df[df['total_value'] > 50000].to_sql('high_value_trades', conn_new, if_exists='replace', index=False)
conn_new.close()


# -------------------------------
# 3️⃣ STREAMLIT DASHBOARD
# -------------------------------

st.title("📊 Trading Dashboard")

# Sidebar stock filter
selected_stock = st.sidebar.selectbox("Select Stock Symbol", df['stock_symbol'].unique())

df_filtered = df[df['stock_symbol'] == selected_stock]

st.subheader(f"📌 Showing data for: **{selected_stock}**")
st.write(df_filtered.head())

# -------------------------------
# 📈 Price Trend Plot
# -------------------------------
st.subheader("📈 Price Trend Over Time")

fig1, ax1 = plt.subplots()
df_filtered_sorted = df_filtered.sort_values("trade_date")
ax1.plot(df_filtered_sorted['trade_date'], df_filtered_sorted['price'])
plt.xticks(rotation=45)
st.pyplot(fig1)


# -------------------------------
# 📊 Total Value per Stock (Bar Chart)
# -------------------------------
st.subheader("💰 Total Trade Value per Stock")

st.bar_chart(stock_summary.set_index('stock_symbol'))


# -------------------------------
# 📊 Histogram – Trade Quantities
# -------------------------------
st.subheader("📦 Distribution of Trade Quantities")

fig2, ax2 = plt.subplots()
ax2.hist(df['quantity'])
ax2.set_xlabel("Quantity")
ax2.set_ylabel("Frequency")
st.pyplot(fig2)


# -------------------------------
# 🥧 Pie Chart – BUY vs SELL
# -------------------------------
st.subheader("🔄 BUY vs SELL Distribution")

txn_counts = df['transaction_type'].value_counts()

fig3, ax3 = plt.subplots()
ax3.pie(txn_counts, labels=txn_counts.index, autopct='%1.1f%%')
st.pyplot(fig3)


st.success("Dashboard Loaded Successfully!")
