from typing import Dict, List

import numpy as np
import pandas as pd

from airflow.decorators import task, task_group



def df_convert_dtypes(df: pd.DataFrame, convert_from: np.dtype, convert_to: np.dtype):
    cols = df.select_dtypes(include=[convert_from]).columns
    for col in cols:
        df[col] = df[col].values.astype(convert_to)
    return df


@task()
def extract() -> Dict[str, pd.DataFrame]:
    """
    #### Extract task
    A simple task that loads each file in the zipped file into a dataframe,
    building a list of dataframes that is returned


    """
    from zipfile import ZipFile
    filename = "/usr/local/airflow/dags/data/energy-consumption-generation-prices-and-weather.zip"
    dfs = [pd.read_csv(ZipFile(filename).open(i)) for i in ZipFile(filename).namelist()]
    return {
        'df_energy': dfs[0],
        'df_weather': dfs[1]
    }


@task
def post_process_energy_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare energy dataframe for merge with weather data
    """


    # Drop columns that are all 0s\
    import pandas as pd
    df = df.drop(['generation fossil coal-derived gas','generation fossil oil shale', 
                  'generation fossil peat', 'generation geothermal', 
                  'generation hydro pumped storage aggregated', 'generation marine', 
                  'generation wind offshore', 'forecast wind offshore eday ahead',
                  'total load forecast', 'forecast solar day ahead',
                  'forecast wind onshore day ahead'], 
                  axis=1)

    # Extract timestamp
    df['time'] = pd.to_datetime(df['time'], utc=True, infer_datetime_format=True)
    df = df.set_index('time')

    # Interpolate the null price values
    df.interpolate(method='linear', limit_direction='forward', inplace=True, axis=0)
    return df


@task
def post_process_weather_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare weather dataframe for merge with energy data
    """


    # Convert all ints to floats
    df = df_convert_dtypes(df, np.int64, np.float64)

    # Extract timestamp 
    df['time'] = pd.to_datetime(df['dt_iso'], utc=True, infer_datetime_format=True)

    # Drop original time column
    df = df.drop(['dt_iso'], axis=1)
    df = df.set_index('time')

    # Reset index and drop records for the same city and time
    df = df.reset_index().drop_duplicates(subset=['time', 'city_name'],
                                                          keep='first').set_index('time')

    # Remove unnecessary qualitiative columns
    df = df.drop(['weather_main', 'weather_id', 
                                  'weather_description', 'weather_icon'], axis=1)

    # Filter out pressure and wind speed outliers
    df.loc[df.pressure > 1051, 'pressure'] = np.nan
    df.loc[df.pressure < 931, 'pressure'] = np.nan
    df.loc[df.wind_speed > 50, 'wind_speed'] = np.nan

    # Interpolate for filtered values
    df.interpolate(method='linear', limit_direction='forward', inplace=True, axis=0)
    return df


@task
def join_dataframes_and_post_process(df_energy: pd.DataFrame, df_weather: pd.DataFrame) -> pd.DataFrame:
    """
    Join dataframes and drop city-specific features
    """


    df_final = df_energy
    df_1, df_2, df_3, df_4, df_5 = [x for _, x in df_weather.groupby('city_name')]
    dfs = [df_1, df_2, df_3, df_4, df_5]

    for df in dfs:
        city = df['city_name'].unique()
        city_str = str(city).replace("'", "").replace('[', '').replace(']', '').replace(' ', '')
        df = df.add_suffix('_{}'.format(city_str))
        df_final = df_final.merge(df, on=['time'], how='outer')
        df_final = df_final.drop('city_name_{}'.format(city_str), axis=1)


    cities = ['Barcelona', 'Bilbao', 'Madrid', 'Seville', 'Valencia']
    for city in cities:
        df_final = df_final.drop(['rain_3h_{}'.format(city)], axis=1)

    return df_final


@task
def add_features(df: pd.DataFrame) -> pd.DataFrame:

    """
    Extract helpful temporal, geographic, and highly correlated energy features
    """
    # Calculate the weight of every city
    total_pop = 6155116 + 5179243 + 1645342 + 1305342 + 987000
    weight_Madrid = 6155116 / total_pop
    weight_Barcelona = 5179243 / total_pop
    weight_Valencia = 1645342 / total_pop
    weight_Seville = 1305342 / total_pop
    weight_Bilbao = 987000 / total_pop
    cities_weights = {'Madrid': weight_Madrid, 
                      'Barcelona': weight_Barcelona,
                      'Valencia': weight_Valencia,
                      'Seville': weight_Seville,
                      'Bilbao': weight_Bilbao}

    for i in range(len(df)):
        # Generate 'hour', 'weekday' and 'month' features
        position = df.index[i]
        hour = position.hour
        weekday = position.weekday()
        month = position.month
        df.loc[position, 'hour'] = hour
        df.loc[position, 'weekday'] = weekday
        df.loc[position, 'month'] = month

        # Generate 'business hour' feature
        if (hour > 8 and hour < 14) or (hour > 16 and hour < 21):
            df.loc[position, 'business hour'] = 2
        elif (hour >= 14 and hour <= 16):
            df.loc[position, 'business hour'] = 1
        else:
            df.loc[position, 'business hour'] = 0
        print("business hours generated")

        # Generate 'weekend' feature

        if (weekday == 6):
            df.loc[position, 'weekday'] = 2
        elif (weekday == 5):
            df.loc[position, 'weekday'] = 1
        else:
            df.loc[position, 'weekday'] = 0
        print("weekdays generated")

        # Generate 'temp_range' for each city
        temp_weighted = 0
        for city in cities_weights.keys():
            temp_max = df.loc[position, 'temp_max_{}'.format(city)]
            temp_min = df.loc[position, 'temp_min_{}'.format(city)]
            df.loc[position, 'temp_range_{}'.format(city)] = abs(temp_max - temp_min)

            # Generated city-weighted temperature 
            temp = df.loc[position, 'temp_{}'.format(city)]
            temp_weighted += temp * cities_weights.get('{}'.format(city))
        df.loc[position, 'temp_weighted'] = temp_weighted

        print("city temp features generated")


    df['generation coal all'] = df['generation fossil hard coal'] + df['generation fossil brown coal/lignite']
    return df


@task
def prepare_model_inputs(df_final: pd.DataFrame):
    """
    Transform each feature to fall within a range from 0 to 1, pull out the target price from the features, 
    and use PCA to reduce the features to those with an explained variance >= 0.80. Concatenate the scaled and 
    dimensionality-reduced feature matrix with the scaled target vector, and return this result.
    matrix with the 
    """
    from sklearn.preprocessing import LabelEncoder, StandardScaler, MinMaxScaler
    from sklearn.decomposition import PCA
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    X = df_final[df_final.columns.drop('price actual')].values
    y = df_final['price actual'].values
    y = y.reshape(-1, 1)
    scaler_X = MinMaxScaler(feature_range=(0, 1))
    scaler_y = MinMaxScaler(feature_range=(0, 1))
    scaler_X.fit(X[:VAL_END_INDEX])
    scaler_y.fit(y[:VAL_END_INDEX])
    X_norm = scaler_X.transform(X)
    y_norm = scaler_y.transform(y)

    pca = PCA(n_components=0.80)
    pca.fit(X_norm[:VAL_END_INDEX])
    X_pca = pca.transform(X_norm)
    dataset_norm = np.concatenate((X_pca, y_norm), axis=1)
    df_norm = pd.DataFrame(dataset_norm)
    client = GCSHook().get_conn()
    # 
    write_bucket = client.bucket(DATASET_NORM_WRITE_BUCKET)
    write_bucket.blob(TRAINING_DATA_PATH).upload_from_string(pd.DataFrame(dataset_norm).to_csv())


@task_group
def join_data_and_add_features():
    """
    Task group responsible for feature engineering, including:
      1. Extracting dataframes from local zipped file
      2. Processing energy and weather dataframes
      3. Joining dataframes
      4. Adding features to the joined dataframe
      5. Producing a dimension-reduced numpy array containing the most
         significant features, and save it to GCS
    """
    output = extract()
    df_energy, df_weather =  output["df_energy"], output["df_weather"]
    df_energy = post_process_energy_df(df_energy)
    df_weather = post_process_weather_df(df_weather)
    df_final = join_dataframes_and_post_process(df_energy, df_weather)
    df_final = add_features(df_final)
    prepare_task = prepare_model_inputs(df_final)