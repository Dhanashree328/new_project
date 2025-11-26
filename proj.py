import sqlite3
import pandas as pd
import random

# Connect to SQLite database
conn = sqlite3.connect('trading.db')
cur = conn.cursor()

# Create table
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

# Generate mock data
stocks = ['AAPL', 'TSLA', 'GOOGL', 'AMZN', 'MSFT']
data = []

for i in range(1, 1001):
    data.append((
        i,  # trade_id
        random.randint(1, 50),  # user_id
        random.choice(stocks),
        random.choice(['BUY', 'SELL']),
        round(random.uniform(100, 1500), 2),
        random.randint(1, 100),
        f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
    ))

# Insert data into SQL
cur.executemany('''
INSERT INTO trades (trade_id, user_id, stock_symbol, transaction_type, price, quantity, trade_date)
VALUES (?, ?, ?, ?, ?, ?, ?)
''', data)

conn.commit()
conn.close()

# Connect and extract
conn = sqlite3.connect('trading.db')
df = pd.read_sql('SELECT * FROM trades', conn)
conn.close()

print(df.head())

#Add a total value column
df['total_value'] = df['price'] * df['quantity']

#Filter high-value trades (> ₹50,000)
high_value_trades = df[df['total_value'] > 50000]

#Aggregate total value per stock

stock_summary = df.groupby('stock_symbol')['total_value'].sum().reset_index()
stock_summary = stock_summary.sort_values(by='total_value', ascending=False)

#Load Transformed Data Back to SQL
conn_new = sqlite3.connect('trading_summary.db')
stock_summary.to_sql('stock_total_value', conn_new, if_exists='replace', index=False)
high_value_trades.to_sql('high_value_trades', conn_new, if_exists='replace', index=False)
conn_new.close()

# Top 3 stocks by total trade value
print(stock_summary.head(3))

# Total number of high-value trades
print(len(high_value_trades))

# Total BUY vs SELL trades
print(df['transaction_type'].value_counts())
