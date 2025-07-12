import json
import os
import hashlib
from typing import Dict, Any, Union, Optional, Set
from bytewax.connectors.kafka import KafkaSource, KafkaSinkMessage, KafkaSink, KafkaSourceMessage, KafkaError
from bytewax.dataflow import Dataflow
import bytewax.operators as op

# Global set to store processed checksums
processed_checksums: Set[str] = set()

def calculate_checksum(data: Dict[str, Any]) -> str:
    """Calculate MD5 checksum for OHLCV data"""
    # Create a consistent string representation for checksum
    checksum_string = f"{data['time']}-{data['symbol']}-{data['open']}-{data['high']}-{data['low']}-{data['close']}-{data['volume']}"
    return hashlib.md5(checksum_string.encode('utf-8')).hexdigest()

def transform_func(msg: Union[KafkaSourceMessage, KafkaError]) -> Optional[Dict[str, Any]]:
    if isinstance(msg, KafkaError):
        print(f"Kafka error: {msg}")
        return None
    
    try:
        raw_data = json.loads(msg.value.decode('utf-8'))
        if 'data' in raw_data and 'k' in raw_data['data']:
            kline_data = raw_data['data']['k']
            data = {
                "time": int(kline_data['t']),  
                "symbol": kline_data['s'],     
                "open": float(kline_data['o']),   
                "high": float(kline_data['h']),   
                "low": float(kline_data['l']),    
                "close": float(kline_data['c']),  
                "volume": float(kline_data['v'])  
            }
            
            # Calculate checksum
            checksum = calculate_checksum(data)
            
            # Check if already processed
            if checksum in processed_checksums:
                print(f"Duplicate data detected for {data['symbol']} at {data['time']}, skipping...")
                return None
                
            # Add to processed set
            processed_checksums.add(checksum)
            print(f"Processing new data for {data['symbol']} at {data['time']}")
            return data
        else:
            print(f"Invalid data format: {raw_data}")
            return None
    except Exception as e:
        print(f"Error parsing message: {e}")
        return None

def to_kafka_message(data: Optional[Dict[str, Any]]) -> Optional[KafkaSinkMessage]:
    """Convert OHLCV data to KafkaSinkMessage"""
    if data is None:
        return None
    return KafkaSinkMessage(
        key=data["symbol"].encode('utf-8'), 
        value=json.dumps(data).encode('utf-8')
    )

brokers = os.environ.get('KAFKA_SERVERS', 'localhost:9092').split(',')  
flow = Dataflow("binance-ohlc-transform")
kinp = op.input("kafka-in", flow, KafkaSource(brokers, ["ohlcv.raw"]))
transformed = op.map("transform", kinp, transform_func)
# Filter out None values (errors and duplicates)
filtered = op.filter("filter-errors", transformed, lambda x: x is not None)
# Convert to KafkaSinkMessage with JSON serialized value
processed = op.map("to-kafka-msg", filtered, to_kafka_message)
# Filter out None messages before sending to Kafka
final_filtered = op.filter("filter-none-messages", processed, lambda x: x is not None)
op.output("kafka-out", final_filtered, KafkaSink(brokers, "ohlcv.processed")) 