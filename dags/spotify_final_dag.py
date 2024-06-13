
import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine, event
from spotify_etl import spotify_etl
import logging
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2024, 5, 30),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

dag = DAG(
    'spotify_final_dag',
    default_args=default_args,
    description='Spotify ETL process',
    schedule_interval=dt.timedelta(minutes=50)
)

def get_postgres_engine():
    try:
        # Retrieve connection object from Airflow's connection manager
        connection = PostgresHook.get_connection('postgre_sql')
        
        # Create a connection string from the connection parameters
        connection_string = f"postgresql+psycopg2://{connection.login}:{connection.password}@{connection.host}:{connection.port}/airflow"
        
        # Create and return the SQLAlchemy engine
        engine = create_engine(connection_string)
        
        @event.listens_for(engine, "connect")
        def set_search_path(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("SET search_path TO public")
            cursor.close()
        
        return engine
    except Exception as e:
        logging.error(f"Failed to create PostgreSQL engine: {e}")
        raise

def ETL():
    try:
        print("ETL process started")
        df = spotify_etl()

        if df is None or df.empty:
            logging.warning("No data returned from spotify_etl.")
            return  # Exit the function if no data is returned

        print(f"Data received from spotify_etl: Preview - {df.head()}")

        engine = get_postgres_engine()
        df.to_sql('my_played_tracks', engine, if_exists='replace', index=False)
        print("Data loaded to PostgreSQL successfully")
    except Exception as e:
        logging.error(f"Error during ETL process: {e}")
        raise



with dag:
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgre_sql',
        sql="""
        CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
        """
    )

    run_etl = PythonOperator(
        task_id='spotify_etl_final',
        python_callable=ETL
    )
    
    create_table >> run_etl

