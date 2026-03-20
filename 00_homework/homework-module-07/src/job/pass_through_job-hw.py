from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_events_source_kafka(t_env):
    table_name = "events"

    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count DOUBLE PRECISION,
            trip_distance DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,

            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),

            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'  -- Add this line to ignore parsing errors
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name

def create_processed_events_sink_postgres(t_env):
    table_name = 'green_processed_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime TIMESTAMP,
            lpep_dropoff_datetime TIMESTAMP,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count DOUBLE PRECISION,
            trip_distance DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            total_amount DOUBLE PRECISION
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    # Force the job to use only 1 slot for now to verify it works
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # # IMPORTANT: Parallelism = 1 (required for your topic)
    # t_env.get_config().set("parallelism.default", "1")

    try:
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_processed_events_sink_postgres(t_env)

        t_env.execute_sql(f"""
            INSERT INTO {postgres_sink}
            SELECT
                event_timestamp AS lpep_pickup_datetime,
                TO_TIMESTAMP(lpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss') AS lpep_dropoff_datetime,
                PULocationID,
                DOLocationID,
                passenger_count,
                trip_distance,
                tip_amount,
                total_amount
            FROM {source_table}
        """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()