import json
import os
import websocket
from datetime import datetime
from confluent_kafka import Producer

kafka_config = {
    'bootstrap.servers': os.environ.get('KAFKA_SERVERS', 'localhost:9092')
}
producer = Producer(kafka_config)

BINANCE_WS_URL = "wss://stream.binance.com/stream"

SUBSCRIBE_MSG = json.dumps({
    "method": "SUBSCRIBE",
    "params": [
        "btcusdt@kline_1s",
        "ethusdt@kline_1s", 
        "bnbusdt@kline_1s",
        "adausdt@kline_1s",
        "solusdt@kline_1s",
        "xrpusdt@kline_1s",
        "dotusdt@kline_1s",
        "dogeusdt@kline_1s",
        "avaxusdt@kline_1s",
        "maticusdt@kline_1s",
        "shibusdt@kline_1s",
        "linkusdt@kline_1s",
        "ltcusdt@kline_1s",
        "uniusdt@kline_1s",
        "atomusdt@kline_1s",
        "etcusdt@kline_1s",
        "xlmusdt@kline_1s",
        "bchusdt@kline_1s",
        "filusdt@kline_1s",
        "trxusdt@kline_1s"
    ],
    "id": 1
})


def on_message(ws, message):
    try:
        data = json.loads(message)
        topic = "ohlcv.raw"
        value = json.dumps(data)
        producer.produce(topic=topic, value=value)
        producer.flush()
        print(f"Published raw data to topic '{topic}'")
    except Exception as e:
        print(f"Error processing message: {e}")


def on_open(ws):
    print("WebSocket opened, subscribing...")
    ws.send(SUBSCRIBE_MSG)


def on_error(ws, error):
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code} - {close_msg}")


if __name__ == "__main__":
    try:
        ws = websocket.WebSocketApp(
            BINANCE_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        print("Starting WebSocket client...")
        ws.run_forever()
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        print("Cleanup completed")
