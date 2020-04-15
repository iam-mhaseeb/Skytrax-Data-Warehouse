import operator
from datetime import datetime, timedelta
from operators import (CreateTablesOperator, SourceToRedshiftOperator)
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

#
# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag,
#     redshift_conn_id='redshift',
#     sql=SqlQueries.songplay_table_insert
# )
#
# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag,
#     append_only=False,
#     table='users',
#     redshift_conn_id='redshift',
#     sql=SqlQueries.user_table_insert
# )
#
# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag,
#     append_only=False,
#     table='songs',
#     redshift_conn_id='redshift',
#     sql=SqlQueries.song_table_insert
# )
#
# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag,
#     append_only=False,
#     table='artists',
#     redshift_conn_id='redshift',
#     sql=SqlQueries.artist_table_insert
# )
#
# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag,
#     append_only=False,
#     table='time',
#     redshift_conn_id='redshift',
#     sql=SqlQueries.time_table_insert
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
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
#
start_operator >> stage_airlines_to_redshift
start_operator >> stage_airports_to_redshift
start_operator >> stage_lounges_to_redshift
start_operator >> stage_seats_to_redshift

# stage_events_to_redshift >> load_songplays_table
# stage_songs_to_redshift >> load_songplays_table
#
# load_songplays_table >> load_user_dimension_table >> run_quality_checks
# load_songplays_table >> load_time_dimension_table >> run_quality_checks
# load_songplays_table >> load_artist_dimension_table >> run_quality_checks
# load_songplays_table >> load_song_dimension_table >> run_quality_checks
#
# run_quality_checks >> end_operator
