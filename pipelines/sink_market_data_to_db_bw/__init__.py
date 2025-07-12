# pipelines/sink_market_data_to_db_bw/__init__.py
import json
import logging
from typing import Union, Optional, Any
from bytewax.connectors.kafka import KafkaSource, KafkaSourceMessage, KafkaError
from bytewax.dataflow import Dataflow
import bytewax.operators as bop
from pipelines import config
from pipelines.sink_market_data_to_db_bw.db_sink import OHLCVSink

KAFKA_TOPIC = "ohlcv.processed"

def parse_ohlcv_message(msg: Union[KafkaSourceMessage, KafkaError]) -> Optional[Any]:
    """Parse OHLCV message từ topic ohlcv.processed"""
    if isinstance(msg, KafkaError):
        logging.error(f"Kafka error: {msg}")
        return None
        
    try:
        data = json.loads(msg.value.decode('utf-8'))
        
        # Validate OHLCV fields
        required_fields = ["time", "symbol", "open", "high", "low", "close", "volume"]
        if all(field in data for field in required_fields):
            # Convert timestamp từ milliseconds sang seconds
            data["time"] = data["time"] / 1000.0
            return data
        else:
            logging.warning(f"Missing OHLCV fields: {data}")
            return None
            
    except Exception as e:
        logging.error(f"Failed to parse OHLCV message: {e}")
        return None

logging.info("Starting OHLCV PostgreSQL Sink Pipeline with checksum deduplication...")
flow = Dataflow("ohlcv-postgresql-sink")
kafka_source = KafkaSource([config.KAFKA_SERVERS], [KAFKA_TOPIC])
input_stream = bop.input("kafka-in", flow, kafka_source)
parsed = bop.map("parse-ohlcv", input_stream, parse_ohlcv_message)
filtered = bop.filter("filter-valid-ohlcv", parsed, lambda x: x is not None)
bop.output("postgresql-sink", filtered, OHLCVSink())
logging.info(f"Pipeline ready - consuming from: {KAFKA_TOPIC}")
logging.info("Target schema: ohlcv_stream, table: ohlcv_data (write-only)")
