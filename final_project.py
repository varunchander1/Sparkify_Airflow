from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),    
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')
    
    drop_staging_events_table = PostgresOperator(
        task_id="drop_staging_events_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.staging_events_table_DROP
    )

    create_staging_events_table = PostgresOperator(
        task_id="create_staging_events_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.staging_events_table_create
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="udacity-dend",
        s3_key="log_data",
        data_format="s3://udacity-dend/log_json_path.json"
    )

    drop_staging_songs_table = PostgresOperator(
        task_id="drop_staging_songs_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.staging_songs_table_DROP
    )

    create_staging_songs_table = PostgresOperator(
        task_id="create_staging_songs_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.staging_songs_table_create
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="udacity-dend",
        s3_key="song_data/",
        data_format='auto'        
    )

    drop_songplay_table = PostgresOperator(
        task_id="drop_songplay_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.songplay_table_DROP
    )

    create_songplay_table = PostgresOperator(
        task_id="songplay_table_create",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.songplay_table_create
    )

    load_songplay_table = LoadFactOperator(
        task_id='Load_songplay_fact_table',
        redshift_conn_id="redshift",
        table="songplays",
        action="reload",
        query=final_project_sql_statements.songplay_table_insert
    )

    drop_user_table = PostgresOperator(
        task_id="drop_user_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.user_table_DROP
    )

    create_user_table = PostgresOperator(
        task_id="user_table_create",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.user_table_create
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table="users",
        action="reload",
        query=final_project_sql_statements.user_table_insert
    )

    drop_song_table = PostgresOperator(
        task_id="drop_song_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.song_table_DROP
    )

    create_song_table = PostgresOperator(
        task_id="song_table_create",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.song_table_create
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table="songs",
        action="reload",
        query=final_project_sql_statements.song_table_insert
    )

    drop_artist_table = PostgresOperator(
        task_id="drop_artist_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.artist_table_drop
    )

    create_artist_table = PostgresOperator(
        task_id="artist_table_create",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.artist_table_create
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table="artists",
        action="reload",
        query=final_project_sql_statements.artist_table_insert

    )

    drop_time_table = PostgresOperator(
        task_id="drop_time_table",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.time_table_DROP
    )

    create_time_table = PostgresOperator(
        task_id="time_table_create",
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.time_table_create
    )


    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table="time",
        action="reload",
        query=final_project_sql_statements.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables = {'table1':['users', 'user_id'], 
                  'table2':['songs', 'song_id'],
                  'table3':['artists', 'artist_id'],
                  'table4':['time', 'start_time'],
                  'table5':['songplays', 'songplay_id']}
    )

    end_operator = DummyOperator(task_id='end_execution')

    drop_staging_events_table >> create_staging_events_table >> stage_events_to_redshift >> drop_songplay_table
    drop_staging_songs_table >> create_staging_songs_table >> stage_songs_to_redshift >> drop_songplay_table
    drop_songplay_table >> create_songplay_table >> load_songplay_table >> run_quality_checks
    load_songplay_table >> drop_user_table >> create_user_table >> load_user_dimension_table >> run_quality_checks
    load_songplay_table >> drop_song_table >> create_song_table >> load_song_dimension_table >> run_quality_checks
    load_songplay_table >> drop_artist_table >> create_artist_table >> load_artist_dimension_table >> run_quality_checks
    load_songplay_table >> drop_time_table >> create_time_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()