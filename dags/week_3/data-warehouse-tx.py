from datetime import datetime

import pandas as pd
from typing import List


from airflow.decorators import dag, task, task_group

PROJECT_ID = "corise-airflow"
DESTINATION_BUCKET = # Modify HERE
BQ_DATASET_NAME = "timeseries_energy"

DATA_TYPES = ["generation", "weather"] 

# Schema for each of the data types
SCHEMAS = {
    "generation": 
        [
            {"name": "time", "type": "STRING", "mode": "NULLABLE"},
            {"name": "generation_biomass", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_fossil_brown_coal_lignite", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_fossil_coal_derived_gas", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_fossil_gas", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_fossil_hard_coal", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_fossil_oil", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_fossil_oil_shale", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_fossil_peat", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_geothermal", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_hydro_pumped_storage_aggregated", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_hydro_pumped_storage_consumption", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_hydro_run_of_river_and_poundage", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_hydro_water_reservoir", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_marine", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_nuclear", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_other", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_other_renewable", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_solar", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_waste", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_wind_offshore", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "generation_wind_onshore", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "forecast_solar_day_ahead", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "forecast_wind_offshore_eday_ahead", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "forecast_wind_onshore_day_ahead", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "total_load_forecast", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "total_load_actual", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "price_day_ahead", "type": "FLOAT64", "mode": "REQUIRED"},
            {"name": "price_actual", "type": "FLOAT64", "mode": "REQUIRED"},
    ],
    "weather": [ 
        {"name": "dt_iso", "type": "STRING", "mode": "NULLABLE"},
        {"name": "city_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "temp", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "temp_min", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "temp_max", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "pressure", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "humidity", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "wind_speed", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "wind_deg", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "rain_1h", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "rain_3h", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "snow_3h", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "clouds_all", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "weather_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "weather_main", "type": "STRING", "mode": "NULLABLE"},
        {"name": "weather_description", "type": "STRING", "mode": "NULLABLE"},
        {"name": "weather_icon", "type": "STRING", "mode": "NULLABLE"}
    ]
} 

normalized_columns = {
    "generation": {
        "time": "time",
        "columns": 
            [   
                "total_load_actual",
                "price_day_ahead",
                "price_actual",
                "generation_fossil_hard_coal",
                "generation_fossil_gas",
                "generation_fossil_brown_coal_lignite",
                "generation_fossil_oil",
                "generation_other_renewable",
                "generation_waste",
                "generation_biomass",
                "generation_other",
                "generation_solar",
                "generation_hydro_water_reservoir",
                "generation_nuclear",
                "generation_hydro_run_of_river_and_poundage",
                "generation_wind_onshore",
                "generation_hydro_pumped_storage_consumption"

            ]
        },
    "weather": {
        "time": "dt_iso",
        "columns": 
            [
                "city_name",
                "temp",
                "pressure",
                "humidity",
                "wind_speed",
                "wind_deg",
                "rain_1h",
                "rain_3h",
                "snow_3h",
                "clouds_all",
            ]
        }
    }


@dag(
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={
        "retries": 2, # If a task fails, it will retry 2 times.
    },
    tags=["example"]) # If set, this tag is shown in the DAG view of the Airflow UI
def data_warehouse_transform_dag():
    """
    ### Data Warehouse Transform DAG
    This DAG performs four operations:
        1. Extracts zip file into two dataframes
        2. Loads these dataframes into parquet files on GCS, with valid column names
        3. Builds external tables on top of these parquet files
        4. Builds normalized views on top of the external tables
        5. Builds a joined view on top of the normalized views, joined on time
    """


    @task
    def extract() -> List[pd.DataFrame]:
        """
        #### Extract task
        A simple task that loads each file in the zipped file into a dataframe,
        building a list of dataframes that is returned


        """
        from zipfile import ZipFile
        filename = "/usr/local/airflow/dags/data/energy-consumption-generation-prices-and-weather.zip"
        dfs = [pd.read_csv(ZipFile(filename).open(i)) for i in ZipFile(filename).namelist()]
        return dfs


    @task
    def load(unzip_result: List[pd.DataFrame]):
        """
        #### Load task
        A simple "load" task that takes in the result of the "extract" task, formats
        columns to be BigQuery-compliant, and writes data to GCS.
        """

        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        client = GCSHook().get_conn()       
        bucket = client.get_bucket(DESTINATION_BUCKET)

        for index, df in enumerate(unzip_result):
            df.columns = df.columns.str.replace(" ", "_")
            df.columns = df.columns.str.replace("/", "_")
            df.columns = df.columns.str.replace("-", "_")
            bucket.blob(f"week-3/{DATA_TYPES[index]}").upload_from_string(df.to_parquet(), "text/parquet")
            print(df.dtypes)

    @task_group
    def create_external_tables():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
        tasks = []

        # TODO Modify here to produce two external tables, one for each data type,
        # using the schemas specified in SCHEMAS. 

        # When using the BigQueryCreateExternalTableOperator, it's suggested you use the table_resource
        # field to specify DDL configuration parameters. If you don't, then you will see an error
        # related to the built table_resource specifying csvOptions even though the desired format is 
        # PARQUET.



    def produce_select_statement(timestamp_column: str, columns: List[str]) -> str:
        # TODO Modify here to produce a select statement by casting 'timestamp_column' to 
        # TIMESTAMP type, and selecting all of the columns in 'columns'


    @task_group
    def produce_normalized_views():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
        # TODO Modify here to produce views for each of the datasources, capturing only the essential
        # columns specified in normalized_columns. A key step at this stage is to convert the relevant 
        # columns in each datasource from string to time. The utility function 'produce_select_statement'
        # accepts the timestamp column, and essential columns for each of the datatypes and build a 
        # select statement ptogrammatically, which can then be passed to the Airflow Operators.


    @task_group
    def produce_joined_view():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
        # TODO Modify here to produce a view that joins the two normalized views on time


    unzip_task = extract()
    load_task = load(unzip_task)
    external_table_task = create_external_tables()
    load_task >> external_table_task
    normal_view_task = produce_normalized_views()
    external_table_task >> normal_view_task
    joined_view_task = produce_joined_view()
    normal_view_task >> joined_view_task


data_warehouse_transform_dag = data_warehouse_transform_dag()