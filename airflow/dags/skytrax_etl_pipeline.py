import operator
from datetime import datetime, timedelta
from operators import (CreateTablesOperator, SourceToRedshiftOperator, LoadDimensionOperator)
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries

from airflow import DAG


default_args = {
    'owner': 'haseeb',
    'start_date': datetime(2020, 4, 12),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('skytrax_etl_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = CreateTablesOperator(
    task_id='Begin_execution',
    dag=dag,
    redshift_conn_id='redshift',
    sql_commands=SqlQueries.create_table_queries
)

stage_airlines_to_redshift = SourceToRedshiftOperator(
    task_id='Stage_Airlines',
    dag=dag,
    table='stagging_airline',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='skytrax-warehouse',
    s3_key='source-data/airline.csv',
    copy_extra="FORMAT AS JSON 'auto' REGION 'us-east-2' TRUNCATECOLUMNS"
)

stage_airports_to_redshift = SourceToRedshiftOperator(
    task_id='Stage_Airports',
    dag=dag,
    table='stagging_airport',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='skytrax-warehouse',
    s3_key='source-data/airport.json',
    copy_extra="FORMAT AS JSON 'auto' REGION 'us-east-2' TRUNCATECOLUMNS"
)

stage_lounges_to_redshift = SourceToRedshiftOperator(
    task_id='Stage_Lounges',
    dag=dag,
    table='stagging_lounge',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='skytrax-warehouse',
    s3_key='source-data/lounge.csv',
    copy_extra="FORMAT AS JSON 'auto' REGION 'us-east-2' TRUNCATECOLUMNS"
)

stage_seats_to_redshift = SourceToRedshiftOperator(
    task_id='Stage_Seats',
    dag=dag,
    table='stagging_seat',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='skytrax-warehouse',
    s3_key='source-data/seat.csv',
    copy_extra="FORMAT AS JSON 'auto' REGION 'us-east-2' TRUNCATECOLUMNS"
)

load_passengers_dimension_table = LoadDimensionOperator(
    task_id='Load_passengers_dim_table',
    dag=dag,
    append_only=False,
    table='passengers',
    redshift_conn_id='redshift',
    sql=SqlQueries.passengers_table_insert
)

load_airports_dimension_table = LoadDimensionOperator(
    task_id='Load_airports_dim_table',
    dag=dag,
    append_only=False,
    table='airports',
    redshift_conn_id='redshift',
    sql=SqlQueries.airports_table_insert
)

load_airlines_dimension_table = LoadDimensionOperator(
    task_id='Load_airlines_dim_table',
    dag=dag,
    append_only=False,
    table='airlines',
    redshift_conn_id='redshift',
    sql=SqlQueries.airlines_table_insert
)

load_aircrafts_dimension_table = LoadDimensionOperator(
    task_id='Load_aircrafts_dim_table',
    dag=dag,
    append_only=False,
    table='aircrafts',
    redshift_conn_id='redshift',
    sql=SqlQueries.aircrafts_table_insert
)

load_lounges_dimension_table = LoadDimensionOperator(
    task_id='Load_lounges_dim_table',
    dag=dag,
    append_only=False,
    table='lounges',
    redshift_conn_id='redshift',
    sql=SqlQueries.lounges_table_insert
)

#
# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     sql=SqlQueries.songplay_table_insert
# )
#

# test_cases = [(SqlQueries.songplay_count_test, operator.gt, 0),
#               (SqlQueries.users_count_test, operator.gt, 0), (SqlQueries.songs_count_test, operator.gt, 0),
#               (SqlQueries.artists_count_test, operator.gt, 0), (SqlQueries.time_count_test, operator.gt, 0),
#               (SqlQueries.users_null_test, operator.eq, 0), (SqlQueries.songs_null_test, operator.eq, 0),
#               (SqlQueries.artists_null_test, operator.eq, 0), (SqlQueries.time_null_test, operator.eq, 0),
#              ]
#
# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag,
#     redshift_conn_id='redshift',
#     test_cases=test_cases
# )
#
finish_dimensional_tables_load = DummyOperator(task_id='Finish_dimensional_tables_load',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_airlines_to_redshift
start_operator >> stage_airports_to_redshift
start_operator >> stage_lounges_to_redshift
start_operator >> stage_seats_to_redshift

stage_airlines_to_redshift >> load_airlines_dimension_table >> finish_dimensional_tables_load
stage_airports_to_redshift >> load_airports_dimension_table >> finish_dimensional_tables_load
stage_lounges_to_redshift >> load_lounges_dimension_table >> finish_dimensional_tables_load
stage_seats_to_redshift >> load_aircrafts_dimension_table >> load_passengers_dimension_table >> finish_dimensional_tables_load

finish_dimensional_tables_load >> end_operator
