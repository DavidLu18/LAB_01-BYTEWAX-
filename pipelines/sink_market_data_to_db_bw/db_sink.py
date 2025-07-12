import logging
from dataclasses import dataclass
from bytewax.outputs import StatelessSinkPartition, DynamicSink
from sqlalchemy import text
from pipelines.resources.postgres import get_db

# New schema for write-only operations
ohlcv_table_creation = """
CREATE SCHEMA IF NOT EXISTS ohlcv_stream;

CREATE TABLE IF NOT EXISTS ohlcv_stream.ohlcv_data (
    time timestamp NOT NULL,
    symbol TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    ingested_at timestamp DEFAULT CURRENT_TIMESTAMP
);

-- 📊 Basic indexes for queries
CREATE INDEX IF NOT EXISTS idx_ohlcv_stream_time ON ohlcv_stream.ohlcv_data (time DESC);
CREATE INDEX IF NOT EXISTS idx_ohlcv_stream_symbol ON ohlcv_stream.ohlcv_data (symbol);
CREATE INDEX IF NOT EXISTS idx_ohlcv_stream_ingested ON ohlcv_stream.ohlcv_data (ingested_at DESC);
"""

# Write-only insert query (removed ON CONFLICT)
ohlcv_insert_query = """
INSERT INTO ohlcv_stream.ohlcv_data (time, symbol, open, high, low, close, volume) 
VALUES (to_timestamp(:time), :symbol, :open, :high, :low, :close, :volume)
"""

stock_foreign_query = """
INSERT INTO trading.stock_foreign_history (
    time, symbol, total_room, current_room,
    buy_vol, sell_vol, buy_val, sell_val
) VALUES (
    :time, :symbol, :total_room, :current_room,
    :buy_vol, :sell_vol, :buy_val, :sell_val
)
"""

@dataclass
class DataQueryFields:
    query: str
    fields: list

_data_type_query_map = {
    "OHLCV": DataQueryFields(
        query=ohlcv_insert_query,
        fields=["time", "symbol", "open", "high", "low", "close", "volume"]
    ),
    "SF": DataQueryFields(
        query=stock_foreign_query,
        fields=["time", "symbol", "total_room", "current_room", "buy_vol", "sell_vol", "buy_val", "sell_val"]
    ),
}


class _OHLCVSinkPartition(StatelessSinkPartition):
    def __init__(self):
        self.db = next(get_db())
        self._ensure_table_exists()
        
    def _ensure_table_exists(self):
        try:
            self.db.execute(text(ohlcv_table_creation))
            self.db.commit()
            logging.info("✅ OHLCV stream schema and table ready")
        except Exception as e:
            logging.warning(f"⚠️ Table creation warning: {e}")

    def write_batch(self, items):
        if not items:
            return
        
        success_count = 0
        for item in items:
            try:
                # Set data type
                item["data_type"] = "OHLCV"
                query_fields = _data_type_query_map["OHLCV"]
                
                # Prepare data
                filtered_item = {k: item[k] for k in query_fields.fields if k in item}
                
                # Insert (write-only, no conflict handling)
                self.db.execute(text(query_fields.query), filtered_item)
                success_count += 1
                
            except Exception as e:
                logging.error(f"❌ Error inserting: {e}")
        
        try:
            self.db.commit()
            logging.info(f"📤 Inserted {success_count} OHLCV records to ohlcv_stream.ohlcv_data")
        except Exception as e:
            logging.error(f"❌ Commit failed: {e}")
            self.db.rollback()

    def close(self):
        """Cleanup"""
        try:
            self.db.close()
        except Exception as e:
            logging.error(f"❌ Error closing: {e}")

class OHLCVSink(DynamicSink):
    """Write-only OHLCV PostgreSQL Sink to new schema"""
    def build(self, step_id: str, worker_index: int, worker_count: int) -> _OHLCVSinkPartition:
        return _OHLCVSinkPartition()

# Legacy MarketDataSink 
class _MarketDataSinkPartition(StatelessSinkPartition):
    def __init__(self):
        self.db = next(get_db())

    def write_batch(self, items):
        if not items:
            return
        for item in items:
            data_type = item.get("data_type")
            query_fields = _data_type_query_map.get(data_type)
            if query_fields:
                filtered_item = {k: item[k] for k in query_fields.fields if k in item}
                self.db.execute(text(query_fields.query), filtered_item)
        self.db.commit()

    def close(self):
        try:
            self.db.close()
        except Exception as e:
            logging.error(f"Error closing: {e}")

class MarketDataSink(DynamicSink):
    def build(self, step_id: str, worker_index: int, worker_count: int) -> _MarketDataSinkPartition:
        return _MarketDataSinkPartition()
