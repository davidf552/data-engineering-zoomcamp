from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_session_source(t_env):
    table_name = "green_trips"
    source_ddl = f"""
    CREATE TABLE {table_name} (
        PULocationID INT,
        DOLocationID INT,
        trip_distance DOUBLE,
        total_amount DOUBLE,
        lpep_pickup_datetime VARCHAR,
        event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
        WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'properties.bootstrap.servers' = 'redpanda:29092',
        'topic' = 'green-trips',
        'scan.startup.mode' = 'earliest-offset',
        'properties.auto.offset.reset' = 'earliest',
        'format' = 'json'
    );
    """
    t_env.execute_sql(source_ddl)
    return table_name

def create_session_sink(t_env):
    table_name = "green_trips_session"
    sink_ddl = f"""
    CREATE TABLE {table_name} (
        session_start TIMESTAMP(3),
        session_end TIMESTAMP(3),
        PULocationID INT,
        num_trips BIGINT,
        PRIMARY KEY (session_start, PULocationID) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = '{table_name}',
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def session_aggregation():
    # setup Flink streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_session_source(t_env)
        sink_table = create_session_sink(t_env)

        # session window aggregation query
        t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            SESSION_START(event_timestamp, INTERVAL '5' MINUTE) AS session_start,
            SESSION_END(event_timestamp, INTERVAL '5' MINUTE) AS session_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM {source_table}
        GROUP BY
            SESSION(event_timestamp, INTERVAL '5' MINUTE),
            PULocationID;
        """).wait()

    except Exception as e:
        print("Session aggregation job failed:", str(e))

if __name__ == "__main__":
    session_aggregation()