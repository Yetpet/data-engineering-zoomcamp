from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def run_session_aggregation():
    # Setup configuration for restart strategy
    config = Configuration()
    config.set_string("restart-strategy.type", "fixed-delay")
    config.set_string("restart-strategy.fixed-delay.attempts", "3")
    config.set_string("restart-strategy.fixed-delay.delay", "10s")

    env = StreamExecutionEnvironment.get_execution_environment()
    # Force the job to use only 1 slot and try to restart 3 times if it fails
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 1. Define Source (Kafka) with Watermark
    t_env.execute_sql("""
        CREATE TABLE kafka_source (
            lpep_pickup_datetime STRING,
            PULocationID INT,
            ts AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'session-agg-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # 2. Define Sink (Postgres)
    t_env.execute_sql("""
        CREATE TABLE postgres_session_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'session_trip_counts',
            'username' = 'postgres',
            'password' = 'postgres'
        )
    """)

    # 1. ADD THIS NEW TABLE DEFINITION HERE
    t_env.execute_sql("""
        CREATE TABLE print_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT
        ) WITH ('connector' = 'print')
    """)

    # 3. Session Window Logic
    # The 'INTERVAL '5' MINUTES' defines the inactivity gap
    t_env.execute_sql("""
        INSERT INTO print_sink
        SELECT 
            window_start, 
            window_end, 
            PULocationID, 
            COUNT(*) as num_trips
        FROM TABLE(
            SESSION(TABLE kafka_source, DESCRIPTOR(ts), INTERVAL '15' SECONDS)
        )
        GROUP BY window_start, window_end, PULocationID
    """)

if __name__ == '__main__':
    run_session_aggregation()