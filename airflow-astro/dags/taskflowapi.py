"""
Apache Airflow introduced the TaskFlow API which allows you to create tasks
using Python decorators like @task. This is a cleaner and more intuitive way
of writing tasks without needing to manually use operators like PythonOperator.
Let me show you how to modify the previous code to use the @task decorator.
"""

from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id='math_sequence_with_taskflow',
    start_date=datetime(2023,1,1),
    schedule='@once',
    catchup=False
) as dag:

    #Task-1: Start with the initial number
    @task
    def start_number():
        initial_value=10
        print(f"Starting with number {initial_value}")
        return initial_value

    @task
    def add_five(current_value):
        new_value=current_value+5
        print(f"Add 5: {current_value}+5={new_value}")
        return new_value

    @task
    def multiply_by_two(current_value):
        new_value=current_value*2
        print(f"Multiply by 2: {current_value}*2={new_value}")
        return new_value

    @task
    def subtract_three(current_value):
        new_value=current_value-3
        print(f"Subtract 3: {current_value}-3={new_value}")
        return new_value

    @task
    def compute_square(current_value):
        new_value=current_value**2
        print(f"Compute square: {current_value}^2={new_value}")
        return new_value

    ##Set the dependencies
    start_value=start_number()
    added_value=add_five(start_value)
    multiplied_value=multiply_by_two(added_value)
    subtracted_value=subtract_three(multiplied_value)
    squared_value=compute_square(subtracted_value)