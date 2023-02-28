
from datetime import datetime
from typing import Dict, List, Tuple, Union

import numpy as np
import pandas as pd
import xgboost as xgb
from airflow.operators.empty import EmptyOperator
from airflow.models.dag import DAG
from airflow.decorators import task, task_group

from common.week_2.model import multivariate_data, train_xgboost, VAL_END_INDEX

from common.week_2.feature_engineering import join_data_and_add_features

TRAINING_DATA_PATH = 'week-2/price_prediction_training_data.csv'
# DATASET_NORM_WRITE_BUCKET = '' # Modify here



@task
def read_dataset_norm():
    """
    Read dataset norm from storage

    """

    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    import io
    client = GCSHook().get_conn()
    read_bucket = client.bucket(DATASET_NORM_WRITE_BUCKET)
    dataset_norm = pd.read_csv(io.BytesIO(read_bucket.blob(TRAINING_DATA_PATH).download_as_bytes())).to_numpy()

    return dataset_norm


@task
def produce_indices() -> List[Tuple[np.ndarray, np.ndarray]]:
    """
    Produce zipped list of training and validation indices

    Each pair of training and validation indices should not overlap, and the
    training indices should never exceed the max of VAL_END_INDEX. This is 
    because the data from VAL_END_INDEX to the end will be used in the test
    dataset downstream from this pipeline.

    The number of pairs produced here will be equivalent to the number of 
    mapped 'format_data_and_train_model' tasks you have. For example, a list of
    size that is simply 
    [np.array(range(start_training_idx, end_training_idx) , range(end_training_idx, end_val_idx))]
    will be produced one mapped task.
    """
    
    # TODO Modify here


@task
def format_data_and_train_model(dataset_norm: np.ndarray,
                                indices: Tuple[np.ndarray, np.ndarray]) -> xgb.Booster:
    """
    Extract training and validation sets and labels, and train a model with a given
    set of training and validation indices
    """
    past_history = 24
    future_target = 0
    train_indices, val_indices = indices
    print(f"train_indices is {train_indices}, val_indices is {val_indices}")
    X_train, y_train = multivariate_data(dataset_norm, train_indices, past_history, future_target, step=1, single_step=True)
    X_val, y_val = multivariate_data(dataset_norm, val_indices, past_history, 
                                     future_target, step=1, single_step=True)
    model = train_xgboost(X_train, y_train, X_val, y_val)
    print(f"Model eval score is {model.best_score}")

    return model


@task
def select_best_model(models: List[xgb.Booster]):
    """
    Select model that generalizes the best against the validation set, and 
    write this to GCS. The best_score is an attribute of the model, and corresponds to
    the highest eval score yielded during training.
    """

   # TODO Modify here


@task_group
def train_and_select_best_model():
    """
    Task group responsible for training XGBoost models to predict energy prices, including:
       1. Reading the dataset norm from GCS
       2. Producing a list of training and validation indices numpy array tuples,  
       3. Mapping each element of that list onto the indices argument of format_data_and_train_model
       4. Calling select_best_model on the output of all of the mapped tasks to select the best model and 
          write it to GCS 

    Using different train/val splits, train multiple models and select the one with the best evaluation score.
    """

    past_history = 24
    future_target = 0
    dataset_norm = read_dataset_norm()

    # TODO: Modify here to select best model and save it to GCS, using above methods including
    # format_data_and_train_model, produce_indices, and select_best_model


with DAG("energy_price_prediction",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['model_training'],
    render_template_as_native_obj=True,
    concurrency=5
    ) as dag:

        group_1 = join_data_and_add_features() 
        group_2 = train_and_select_best_model()
        group_1 >> group_2
 