#import dependencies

import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
plt.style.use('fivethirtyeight')
sns.set()
import plotly.offline as py
import plotly.graph_objs as go
import xgboost as xgb
import lightgbm as lgb
from sklearn.model_selection import train_test_split

# Load the Train and Test data
def load_data(datapath):
    data = pd.read_csv(datapath)
    # Dimensions
    print('Shape:', data.shape)
    return data
    
# change the path accordingly    
train_df = load_data('./train/features.csv')
test_df = load_data('./test/features.csv')


# Preprocess function 
def preprocessing_data(train_data):
    train_data['date'] = pd.to_datetime(train_data['date'])
    
    #time features
    train_data['month'] = train_data['date'].dt.month
    train_data['day'] = train_data['date'].dt.dayofweek
    train_data['year'] = train_data['date'].dt.year
    
    #feature engineering
    
    #lag features
    train_data['lag_t28'] = train_data.groupby(['store','item'])['sales'].transform(lambda x: x.shift(28))
    train_data['lag_t29'] = train_data.groupby(['store','item'])['sales'].transform(lambda x: x.shift(29))
    train_data['lag_t30'] = train_data.groupby(['store','item'])['sales'].transform(lambda x: x.shift(30))
    
    #rolling window features
    #mean
    train_data['rolling_mean_weekly'] = train_data['sales'].rolling(window=7).mean()
    train_data['rolling_mean_monthly'] = train_data['sales'].rolling(window=30).mean()
    
    #standard deviation
    train_data['rolling_std_weekly'] = train_data['sales'].rolling(window=7).std()
    train_data['rolling_std_monthly'] = train_data['sales'].rolling(window=30).std()
    
    
    col = [i for i in train_data.columns if i not in ['date','id']]
    y = 'sales'

    return train_data, col, y




train_data, col, y = preprocessing_data(train_df)


########################################################################################
# split into train and validation and test data
train_x, test_x, train_y, test_y = train_test_split(train_data[col],train_data[y], test_size=0.3, random_state=2018)
val_x, test_x, val_y, test_y = train_test_split(test_x[col],test_y, test_size=0.33, random_state=2018)

# Train function
def train_model(train_x,train_y,test_x,test_y,col):
    params = {
        'nthread': 10,
        'max_depth': 5,
        'task': 'train',
        'boosting_type': 'gbdt',
        'objective': 'regression_l1',
        'metric': 'mape', # this is abs(a-e)/max(1,a)
        'num_leaves': 64,
        'learning_rate': 0.2,
        'feature_fraction': 0.9,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'lambda_l1': 3.097758978478437,
        'lambda_l2': 2.9482537987198496,
        'verbose': 1,
        'min_child_weight': 6.996211413900573,
        'min_split_gain': 0.037310344962162616,
        }
    
    lgb_train = lgb.Dataset(train_x,train_y)
    lgb_valid = lgb.Dataset(test_x,test_y)
    model = lgb.train(params, lgb_train, 30, valid_sets=[lgb_train, lgb_valid],
        early_stopping_rounds=50, verbose_eval=50)
    return model

model = train_model(train_x,train_y,test_x,test_y,col)

# Predict
predicted = model.predict(test_x[col])
# save predicted sales to csv file
test_x['sales'] = test_y
test_x['Predicted_sales'] = predicted
test_x['error'] = test_x['sales']- test_x['Predicted_sales']
test_x.to_csv('./Predicted_Sales.csv')
