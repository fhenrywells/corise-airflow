from typing import Tuple

import numpy as np
import xgboost as xgb


def multivariate_data(dataset: np.ndarray,
                      data_indices: np.ndarray,
                      history_size: int,
                      target_size: int,
                      step: int, 
                      single_step=False) -> Tuple[np.ndarray, np.ndarray]:
    """
    Produce subset of dataset indexed by data_indices, with a window size of history_size hours
    """

    import numpy as np
    target = dataset[:, -1]
    data = []
    labels = []
    for i in data_indices:
        indices = range(i, i + history_size, step)
        # If within the last 23 hours in the dataset, skip 
        if i + history_size > len(dataset) - 1:
            continue
        data.append(dataset[indices])
        if single_step:
            labels.append(target[i + target_size])
        else:
            labels.append(target[i : i + target_size])
    return np.array(data), np.array(labels)


def train_xgboost(X_train: np.ndarray,
                  y_train: np.ndarray,
                  X_val: np.ndarray,
                  y_val: np.ndarray) -> xgb.Booster:
    """
    Train xgboost model using training set and evaluated against evaluation set, using 
        a set of model parameters
    """

    X_train_xgb = X_train.reshape(-1, X_train.shape[1] * X_train.shape[2])
    X_val_xgb = X_val.reshape(-1, X_val.shape[1] * X_val.shape[2])
    param = {'eta': 0.03, 'max_depth': 180, 
             'subsample': 1.0, 'colsample_bytree': 0.95, 
             'alpha': 0.1, 'lambda': 0.15, 'gamma': 0.1,
             'objective': 'reg:linear', 'eval_metric': 'rmse', 
             'silent': 1, 'min_child_weight': 0.1, 'n_jobs': -1}
    dtrain = xgb.DMatrix(X_train_xgb, y_train)
    dval = xgb.DMatrix(X_val_xgb, y_val)
    eval_list = [(dtrain, 'train'), (dval, 'eval')]
    xgb_model = xgb.train(param, dtrain, 10, eval_list, early_stopping_rounds=3)
    return xgb_model