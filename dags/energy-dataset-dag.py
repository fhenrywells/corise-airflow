from datetime import datetime

from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API


@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    schedule_interval="@daily",
    # This DAG is set to run for the first time on January 1, 2021. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on scheduler_interval
    start_date=datetime(2021, 1, 1),
    # When catchup=False, your DAG will only run for the latest schedule_interval. In this case, this means
    # that tasks will not be run between January 1, 2021 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the schedule_interval
    catchup=False,
    default_args={
        "retries": 2, # If a task fails, it will retry 2 times.
    },
    tags=['example']) # If set, this tag is shown in the DAG view of the Airflow UI
def energy_dataset_dag():
    """
    ### Basic ETL Dag
    This is a simple ETL data pipeline example that demonstrates the use of
    the TaskFlow API using three simple tasks for scrape, extract, and load.
    For more information on Airflow's TaskFlow API, reference documentation here:
    https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html
    """

    @task()
    def scrape():
        """
        #### Scrape task
        A scrape task to get data ready for the rest of the
        pipeline. In this case, we will be downloading a zipped file from Kaggle
        containing csv energy generation and timeseries data
        """
        import os
        # TODO Download data from Kaggle API
        # cli cmd to download: 
        # kaggle datasets download nicholasjhana/energy-consumption-generation-prices-and-weather


    @task()
    def extract() -> "List[pd.DataFrame]":
        """
        #### Extract task
        A simple task that loads each file in the zipped file into a dataframe,
        building a list of dataframes that is returned


        """
        import pandas as pd
        from zipfile import ZipFile
        # TODO Unzip files into pandas dataframes


    @task()
    def load(unzip_result: "List[pd.DataFrame]"):
        """
        #### Load task
        A simple "load" task that takes in the result of the "transform" task, prints out the 
        schema, and then loads the data into GCS
        """

        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        data_types = ['generation', 'weather']
        client = GCSHook().get_conn()       \
        # TODO Add GCS upload code


    # TODO Add task linking logic here


energy_dataset_dag = energy_dataset_dag()