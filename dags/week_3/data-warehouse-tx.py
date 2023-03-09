from datetime import datetime

import pandas as pd
from typing import List

from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task, task_group

PROJECT_ID = "corise-airflow"
DESTINATION_BUCKET = "corise-airflow-kod"
BQ_DATASET_NAME = "timeseries_energy"

DATA_TYPES = ["generation", "weather"]

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
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)
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
            bucket.blob(f"week-3/{DATA_TYPES[index]}.parquet").upload_from_string(df.to_parquet(), "text/parquet")
            print(df.dtypes)

    @task_group
    def create_bigquery_dataset():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id="create_dataset",
            dataset_id=BQ_DATASET_NAME
        )

    @task_group
    def create_external_tables():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
        EmptyOperator(task_id='placeholder')

        # When using the BigQueryCreateExternalTableOperator, it's suggested you use the table_resource
        # field to specify DDL configuration parameters. If you don't, then you will see an error
        # related to the built table_resource specifying csvOptions even though the desired format is 
        # PARQUET.

        for file_name in DATA_TYPES:
            create_external_table = BigQueryCreateExternalTableOperator(
                task_id=f"{file_name}_create_external_table",
                destination_project_dataset_table=f"{BQ_DATASET_NAME}.external_table",
                bucket=DESTINATION_BUCKET,
                source_objects=[f"week-3/{file_name}.parquet"],
                schema_fields=[normalized_columns],
            )

    def produce_select_statement(timestamp_column: str, columns: List[str]) -> str:
        # TIMESTAMP type, and selecting all of the columns in 'columns'
        select_statement = "SELECT "
        select_statement += f"TIMESTAMP({timestamp_column}) as time"
        for column in columns:
            select_statement += f", {column}"
        return select_statement

    @task_group
    def produce_normalized_views():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
        tasks = []
        for file_name in DATA_TYPES:

            select_statement = produce_select_statement(
                normalized_columns[file_name]["time"],
                normalized_columns[file_name]["columns"]
            )

            tasks.append(
                BigQueryCreateEmptyTableOperator(
                    task_id=f"{file_name}_create_view",
                    dataset_id=BQ_DATASET_NAME,
                    table_id=f"{file_name}_normalized_view",
                    view={
                        "query": f"{select_statement} FROM {PROJECT_ID}.{BQ_DATASET_NAME}.{file_name}_external_table",
                        "useLegacySql": False
                    },
                )
            )


    @task_group
    def produce_joined_view():
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
        produced_joined_view = BigQueryCreateEmptyTableOperator(
                    task_id="produced_joined_view",
                    dataset_id=BQ_DATASET_NAME,
                    table_id="merged_view",
                    view={
                        "query": f"""
                         SELECT energy.*, weather.* except (time) FROM {PROJECT_ID}.{BQ_DATASET_NAME}.generation_normalized_view energy
                         JOIN {PROJECT_ID}.{BQ_DATASET_NAME}.weather_normalized_view weather
                         ON energy.time = weather.time; 
                         """,
                        "useLegacySql": False
                    }
                )
    unzip_task = extract()
    load_task = load(unzip_task)
    create_bigquery_dataset_task = create_bigquery_dataset()
    load_task >> create_bigquery_dataset_task
    external_table_task = create_external_tables()
    create_bigquery_dataset_task >> external_table_task
    normal_view_task = produce_normalized_views()
    external_table_task >> normal_view_task
    joined_view_task = produce_joined_view()
    normal_view_task >> joined_view_task


data_warehouse_transform_dag = data_warehouse_transform_dag()
