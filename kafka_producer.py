import yfinance as yf
from kafka import KafkaProducer
import json
import time

# Configures the kafka server 
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  
    value_serializer=lambda x: json.dumps(x).encode('utf-8')  
)

def fetch_and_send_data(ticker):
    """Fetch stock data and send it to Kafka topic"""
    while True:
        stock = yf.Ticker(ticker)
        data = stock.history(period="1d", interval="1m").tail(1)  

        # Extracts the infromation from Yahoo Finance
        if not data.empty:
            latest = data.iloc[-1]
            message = {
                "ticker": ticker,
                "time": str(latest.name),  
                "price": float(latest['Close']),  
                "volume": int(latest['Volume'])  
            }

            producer.send("stock-stream", value=message)
            print(f"Sent to Kafka: {message}")

        time.sleep(1)

if __name__ == "__main__":
    ticker_input = input("Enter the stock ticker (e.g., AAPL, TSLA): ").upper()
    fetch_and_send_data(ticker_input)  