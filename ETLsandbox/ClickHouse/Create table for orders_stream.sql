-- Для схемы ниже собираем таблицу для хранения данных
-- root
--  |-- key_id: long (nullable = true)
--  |-- operation: string (nullable = true)
--  |-- id: long (nullable = true)
--  |-- order_id: long (nullable = true)
--  |-- status: string (nullable = true)
--  |-- ts_data: long (nullable = true)
--  |-- ts_event_ns: long (nullable = true)
DROP TABLE IF EXISTS default.orders_stream;
CREATE TABLE IF NOT EXISTS default.orders_stream (
    key_id BIGINT,
    operation String,
    id BIGINT,
    order_id BIGINT,
    status String,
    ts_data BIGINT,
    ts_event_ns BIGINT
)
ENGINE = MergeTree()
ORDER BY (key_id, ts_data);