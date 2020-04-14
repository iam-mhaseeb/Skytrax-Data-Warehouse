import operator
from datetime import datetime, timedelta
# from operators import (StageToRedshiftOperator, LoadFactOperator,
#                        LoadDimensionOperator, DataQualityOperator, CreateTablesOperator)
from operators import (CreateTablesOperator)
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
    sql_commands=SqlQueries.clean_up_quries
)
#
# json_path = "s3://{}/{}".format('skytrax-warehouse/source-data', 'log_json_path.json')
# stage_events_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_events',
#     dag=dag,
#     table='staging_events',
#     redshift_conn_id='redshift',
#     aws_credentials_id='aws_credentials',
#     s3_bucket='udacity-dend',
#     s3_key='log_data/2018/11/',
#     copy_extra="FORMAT AS JSON '{}' REGION 'us-west-2'".format(json_path)
# )
#
# stage_songs_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_songs',
#     dag=dag,
#     table='staging_songs',
#     redshift_conn_id='redshift',
#     aws_credentials_id='aws_credentials',
#     s3_bucket='udacity-dend',
#     s3_key='song_data/A/A/',
#     copy_extra="FORMAT AS JSON 'auto' REGION 'us-west-2' TRUNCATECOLUMNS"
# )
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
# start_operator >> stage_events_to_redshift
# start_operator >> stage_songs_to_redshift
#
# stage_events_to_redshift >> load_songplays_table
# stage_songs_to_redshift >> load_songplays_table
#
# load_songplays_table >> load_user_dimension_table >> run_quality_checks
# load_songplays_table >> load_time_dimension_table >> run_quality_checks
# load_songplays_table >> load_artist_dimension_table >> run_quality_checks
# load_songplays_table >> load_song_dimension_table >> run_quality_checks
#
# run_quality_checks >> end_operator

start_operator >> end_operator