from pendulum import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from extract.ECB_api import ExtractECB


default_args = {"owner": "airflow", "start_date": datetime(2021, 1, 1)}


@dag(dag_id=config_dag_id,
     schedule=None,
     default_args=default_args)
def taskflow():
    create_schema = PostgresOperator(
        task_id='create_schema',
        postgres_conn_id='postgre_sql',
        sql='''
            CREATE SCHEMA IF NOT EXISTS ECB
            '''
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgre_sql',
        sql='''
            CREATE TABLE IF NOT EXISTS ECB.config_table_name(
            config_columns)
            '''
    )

    @task()
    def extract():
        parameters = {
            'startPeriod': config_start_period,
            'endPeriod': config_end_period
        }
        ecb = ExtractECB(flowref='FM',
                         keys='M.U2.EUR.RT.MM.EURIBOR1MD_.HSTA',
                         parameters=parameters)
        filename = ecb.main()
        return filename

    extract_data = extract()

    copy_csv = PostgresOperator(
        task_id='copy_csv',
        postgres_conn_id='postgre_sql',
        sql=f'''
            COPY ECB.config_table_name(config_copy)
            FROM '{extract_data}'
            DELIMITER ','
            CSV HEADER
            '''
    )

    create_schema >> create_table >> extract_data >> copy_csv


dag = taskflow()
