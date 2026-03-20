from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def run_window_aggregation():
    # 1. Setup Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 2. Define Source (Kafka)
    # Note: We must define the 'event_timestamp' and 'WATERMARK' for windowing to work
    t_env.execute_sql("""
        CREATE TABLE kafka_source (
            lpep_pickup_datetime STRING,
            PULocationID INT,
            passenger_count DOUBLE,
            ts AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'window-agg-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # 3. Define Sink (Postgres)
    t_env.execute_sql("""
        CREATE TABLE postgres_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'processed_trip_counts',
            'username' = 'postgres',
            'password' = 'postgres'
        )
    """)

    # 4. Perform Window Aggregation and Insert
    # Using the new Flink SQL Window TVF syntax (TUMBLE)
    t_env.execute_sql("""
        INSERT INTO postgres_sink
        SELECT 
            window_start, 
            window_end, 
            PULocationID, 
            COUNT(*) as num_trips
        FROM TABLE(
            TUMBLE(TABLE kafka_source, DESCRIPTOR(ts), INTERVAL '5' MINUTES)
        )
        GROUP BY window_start, window_end, PULocationID
    """).wait()

if __name__ == '__main__':
    run_window_aggregation()