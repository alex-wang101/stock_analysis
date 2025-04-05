import yfinance as yf
from kafka import KafkaProducer
import json
import time
import requests

# Function to fetch the exchange rate
def exchange_rate(base="USD", target="CAD"):
    """Fetch exchange rate from ExchangeRate-API"""
    response = requests.get(f"https://api.exchangerate-api.com/v4/latest/{base}")
    data = response.json()
    return data["rates"].get(target, 1.0)  

# Configures the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  
    value_serializer=lambda x: json.dumps(x).encode('utf-8')  
)

def fetch_and_send_data(ticker, currency_value="USD"):
    """Fetch stock data and send it to Kafka topic"""
    while True:
        stock = yf.Ticker(ticker)
        data = stock.history(period="1d", interval="1m").tail(1)  

        # Extract information from Yahoo Finance
        if not data.empty:
            latest = data.iloc[-1]
            price_usd = float(latest['Close'])

            # Fetch the exchange rate from USD (or other base) to the desired currency
            rate = exchange_rate(base=currency_value, target="CAD")
            price_converted = price_usd * rate

            message = {
                "ticker": ticker,
                "time": str(latest.name),  
                "price_usd": price_usd,  
                "price_converted": price_converted,  
                "volume": int(latest['Volume'])  
            }

            producer.send("stock-stream", value=message)

            # Print each element in the message with explanations
            print(f"Ticker: {message['ticker']}")
            print(f"Time: {message['time']}")
            print(f"Price in USD: {message['price_usd']}")
            print(f"Price in {currency_value}: {message['price_converted']}")
            print(f"Volume: {message['volume']}")
            print(f"Currency exchange rate from {currency_value} to CAD: {rate}")
            print("-" * 50)

        time.sleep(1)  

if __name__ == "__main__":
    ticker_input = input("Enter the stock ticker (e.g., AAPL, TSLA): ").upper()
    currency_value = input("Enter the currency you want to exchange from (e.g., USD): ").upper()
    fetch_and_send_data(ticker_input, currency_value)
