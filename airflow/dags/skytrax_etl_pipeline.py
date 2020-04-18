import operator
from datetime import datetime, timedelta
from operators import (CreateTablesOperator, SourceToRedshiftOperator, LoadDimensionOperator, DataQualityOperator, LoadFactOperator)
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
    columns="""airline_name,link,title,author,author_country,review_date,review_content,aircraft,type_traveller,cabin_flown,
               route,overall_rating,seat_comfort_rating,cabin_staff_rating,food_beverages_rating,inflight_entertainment_rating,
               ground_service_rating,wifi_connectivity_rating,value_money_rating,recommended""",
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='skytrax-warehouse',
    s3_key='source-data/airline.csv',
    copy_extra="FORMAT AS CSV  REGION 'us-east-2' TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL ACCEPTANYDATE DATEFORMAT 'auto' IGNOREHEADER 1"
)

stage_airports_to_redshift = SourceToRedshiftOperator(
    task_id='Stage_Airports',
    dag=dag,
    table='stagging_airport',
    columns="""airport_name,link,title,author,author_country,review_date,review_content,experience_airport,date_visit,type_traveller,
               overall_rating,queuing_rating,terminal_cleanness_rating,terminal_seating_rating,terminal_signs_rating,
               food_beverages_rating,airport_shopping_rating,wifi_connectivity_rating,airport_staff_rating,recommended""",
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='skytrax-warehouse',
    s3_key='source-data/airports/',
    copy_extra="FORMAT AS JSON 'auto'  REGION 'us-east-2' TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL ACCEPTANYDATE DATEFORMAT 'auto'"
)

stage_lounges_to_redshift = SourceToRedshiftOperator(
    task_id='Stage_Lounges',
    dag=dag,
    table='stagging_lounge',
    columns="""airline_name,link,title,author,author_country,review_date,review_content,lounge_name,airport,lounge_type,
               date_visit,type_traveller,overall_rating,comfort_rating,cleanness_rating,bar_beverages_rating,
               catering_rating,washrooms_rating,wifi_connectivity_rating,staff_service_rating,recommended""",
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='skytrax-warehouse',
    s3_key='source-data/lounge.csv',
    copy_extra="FORMAT AS CSV  REGION 'us-east-2' TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL ACCEPTANYDATE DATEFORMAT 'auto' IGNOREHEADER 1"
)

stage_seats_to_redshift = SourceToRedshiftOperator(
    task_id='Stage_Seats',
    dag=dag,
    table='stagging_seat',
    columns="""airline_name,link,title,author,author_country,review_date,review_content,aircraft,seat_layout,date_flown,
               cabin_flown,type_traveller,overall_rating,seat_legroom_rating,seat_recline_rating,seat_width_rating,
               aisle_space_rating,viewing_tv_rating,power_supply_rating,seat_storage_rating,recommended""",
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='skytrax-warehouse',
    s3_key='source-data/seat.csv',
    copy_extra="FORMAT AS CSV  REGION 'us-east-2' TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL ACCEPTANYDATE DATEFORMAT 'auto' IGNOREHEADER 1"
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


load_fact_ratings_table = LoadFactOperator(
    task_id='Load_fact_ratings_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.fact_ratings_table_insert
)


ensure_data_load_in_dims = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    test_cases=[
        (SqlQueries.airlines_count_test, operator.gt, 0),
        (SqlQueries.aircrafts_count_test, operator.gt, 0),
        (SqlQueries.lounges_count_test, operator.gt, 0),
        (SqlQueries.aircrafts_count_test, operator.gt, 0),
        (SqlQueries.aircrafts_count_test, operator.gt, 0),
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_airlines_to_redshift
start_operator >> stage_airports_to_redshift
start_operator >> stage_lounges_to_redshift
start_operator >> stage_seats_to_redshift

stage_airlines_to_redshift >> load_airlines_dimension_table >> load_aircrafts_dimension_table >> load_passengers_dimension_table
stage_airports_to_redshift >> load_airports_dimension_table >> load_lounges_dimension_table >> load_passengers_dimension_table
stage_lounges_to_redshift >> load_passengers_dimension_table
stage_seats_to_redshift >> load_passengers_dimension_table
load_passengers_dimension_table >> ensure_data_load_in_dims

ensure_data_load_in_dims >> load_fact_ratings_table

load_fact_ratings_table >> end_operator
