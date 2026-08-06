from airflow import DAG as d
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook 
import pendulum
start_date=pendulum.datetime(2024, 1, 1, tz="UTC")
import json


#Define the DAG
with d(
    dag_id='nasa_apod_postgres',
    start_date=start_date,
    schedule='@daily',
    catchup=False
) as dag:

    #Step-1: Create the table if it does not exist
    @task
    def create_table():
        #Initialize the postgres hook
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")
        #Create the table
        create_table_query="""
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL  PRIMARY  KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        postgres_hook.run(create_table_query)

    #Step-2: Extract the nasa api data
    extract_apod=HttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api', ## Connection ID Defined In Airfl
        endpoint='planetary/apod', ## NASA API enpoint for APOD
        method='GET',
        data={"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"},
        response_filter=lambda response:response.json(),
    )



    #Step-3: Transform the data (pick the information that I need to save)
    @task
    def transform_apod_data(response):
        apod_data={
            'title': response.get('title',''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url',''),
            'date': response.get('date', ""),
            'media_type': response.get('media_type','')
        }
        return apod_data


    #Step-4: Load the data into Postgres SQL
    @task
    def load_data_to_postgres(apod_data):
        #Initialize the PostgresHook
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")

        #Define the SQL insert query
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s,%s,%s,%s); 
        """
        postgres_hook.run(insert_query, parameters=(apod_data['title'], apod_data['explanation'], apod_data['url'], apod_data['date'], apod_data['media_type']))

    #Step-5: Verify the data with DBViewwer


    #Step-6: Define the task dependencies
    #Extract
    create_table() >> extract_apod ## Ensure the table is created before etraction
    api_response=extract_apod.output
    #Transform
    transformed_data=transform_apod_data(api_response)
    #Load
    load_data_to_postgres(transformed_data)