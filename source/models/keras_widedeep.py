# pylint: disable=C0321,C0103,C0301,E1305,E1121,C0302,C0330,C0111,W0613,W0611,R1705
# -*- coding: utf-8 -*-
"""
ipython source/models/keras_widedeep.py  test  --pdb


python keras_widedeep.py  test

pip install Keras==2.4.3


"""
import os, pandas as pd, numpy as np, sklearn, copy
from sklearn.model_selection import train_test_split

import tensorflow as tf
try :
  import keras
  from keras.callbacks import EarlyStopping, ModelCheckpoint
  from keras import layers
except :
  from tensorflow import keras
  from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint
  from tensorflow.keras import layers

####################################################################################################
verbosity =2

def log(*s):
    print(*s, flush=True)

def log2(*s):
    if verbosity >= 2 :
      print(*s, flush=True)


####################################################################################################
global model, session

def init(*kw, **kwargs):
    global model, session
    model = Model(*kw, **kwargs)
    session = None


cols_ref_formodel = ['cols_cross_input', 'cols_deep_input', 'cols_deep_input']

'''def Modelcustom(n_wide_cross, n_wide,n_deep, n_feat=8, m_EMBEDDING=10, loss='mse', metric = 'mean_squared_error'):

        #### Wide model with the functional API
        col_wide_cross          = layers.Input(shape=(n_wide_cross,))
        col_wide                = layers.Input(shape=(n_wide,))
        merged_layer            = layers.concatenate([col_wide_cross, col_wide])
        merged_layer            = layers.Dense(15, activation='relu')(merged_layer)
        predictions             = layers.Dense(1)(merged_layer)
        wide_model              = keras.Model(inputs=[col_wide_cross, col_wide], outputs=predictions)

        wide_model.compile(loss = 'mse', optimizer='adam', metrics=[ metric ])
        log2(wide_model.summary())

        #### Deep model with the Functional API
        deep_inputs             = layers.Input(shape=(n_deep,))
        embedding               = layers.Embedding(n_feat, m_EMBEDDING, input_length= n_deep)(deep_inputs)
        embedding               = layers.Flatten()(embedding)

        merged_layer            = layers.Dense(15, activation='relu')(embedding)

        embed_out               = layers.Dense(1)(merged_layer)
        deep_model              = keras.Model(inputs=deep_inputs, outputs=embed_out)
        deep_model.compile(loss='mse',   optimizer='adam',  metrics=[ metric ])
        log2(deep_model.summary())


        #### Combine wide and deep into one model
        merged_out = layers.concatenate([wide_model.output, deep_model.output])
        merged_out = layers.Dense(1)(merged_out)
        model      = keras.Model( wide_model.input + [deep_model.input], merged_out)
        model.compile(loss=loss,   optimizer='adam',  metrics=[ metric ])
        log2(model.summary())

        return model
'''

def get_dataset_tuple(Xtrain, cols_type_received, cols_ref):
    """  Split into Tuples to feed  Xyuple = (df1, df2, df3)
    :param Xtrain:
    :param cols_type_received:
    :param cols_ref:
    :return:
    """
    if len(cols_ref) < 1 :
        return Xtrain

    Xtuple_train = []
    for cols_groupname in cols_ref :
        assert cols_groupname in cols_type_received, "Error missing colgroup in config data_pars[cols_model_type] "
        cols_i = cols_type_received[cols_groupname]
        Xtuple_train.append( Xtrain[cols_i] )

    if len(cols_ref) == 1 :
        return Xtuple_train[0]  ### No tuple
    else :
        return Xtuple_train


def get_dataset(data_pars=None, task_type="train", **kw):
    """
      return tuple of dataframes
    """
    # log(data_pars)
    data_type = data_pars.get('type', 'ram')
    cols_ref  = cols_ref_formodel

    if data_type == "ram":
        # cols_ref_formodel = ['cols_cross_input', 'cols_deep_input', 'cols_deep_input' ]
        ### dict  colgroup ---> list of colname
        cols_type_received     = data_pars.get('cols_model_type2', {} )  ##3 Sparse, Continuous

        if task_type == "predict":
            d = data_pars[task_type]
            Xtrain       = d["X"]
            Xtuple_train = get_dataset_tuple(Xtrain, cols_type_received, cols_ref)
            return Xtuple_train

        if task_type == "eval":
            d = data_pars[task_type]
            Xtrain, ytrain  = d["X"], d["y"]
            Xtuple_train    = get_dataset_tuple(Xtrain, cols_type_received, cols_ref)
            return Xtuple_train, ytrain

        if task_type == "train":
            d = data_pars[task_type]
            Xtrain, ytrain, Xtest, ytest  = d["Xtrain"], d["ytrain"], d["Xtest"], d["ytest"]

            ### dict  colgroup ---> list of df
            Xtuple_train = get_dataset_tuple(Xtrain, cols_type_received, cols_ref)
            Xtuple_test  = get_dataset_tuple(Xtest, cols_type_received, cols_ref)


            log2("Xtuple_train", Xtuple_train)

            return Xtuple_train, ytrain, Xtuple_test, ytest


    elif data_type == "file":
        raise Exception(f' {data_type} data_type Not implemented ')

    raise Exception(f' Requires  Xtrain", "Xtest", "ytrain", "ytest" ')


########################################################################################################
########################### Using Sparse Tensor  #######################################################
def ModelCustom2():
    # Build a wide-and-deep model.
    def wide_and_deep_classifier(inputs, linear_feature_columns, dnn_feature_columns, dnn_hidden_units):
        deep = tf.keras.layers.DenseFeatures(dnn_feature_columns, name='deep_inputs')(inputs)
        layers = [int(x) for x in dnn_hidden_units.split(',')]
        for layerno, numnodes in enumerate(layers):
            deep = tf.keras.layers.Dense(numnodes, activation='relu', name='dnn_{}'.format(layerno+1))(deep)
        wide = tf.keras.layers.DenseFeatures(linear_feature_columns, name='wide_inputs')(inputs)
        both = tf.keras.layers.concatenate([deep, wide], name='both')
        output = tf.keras.layers.Dense(1, activation='sigmoid', name='pred')(both)
        model = tf.keras.Model(inputs, output)
        model.compile(optimizer='adam',
                      loss='binary_crossentropy',
                      metrics=['accuracy'])
        return model

    sparse, real =  input_template_feed_keras(cols_type_received, cols_ref)

    DNN_HIDDEN_UNITS = 10
    model = wide_and_deep_classifier(
        inputs,
        linear_feature_columns = sparse.values(),
        dnn_feature_columns = real.values(),
        dnn_hidden_units = DNN_HIDDEN_UNITS)
    #tf.keras.utils.plot_model(model, 'flights_model.png', show_shapes=False, rankdir='LR')
    return model


def input_template_feed_keras(Xtrain, cols_type_received, cols_ref, **kw):
    """
       Create sparse data struccture in KERAS  To plug with MODEL:
       No data, just virtual data
    https://github.com/GoogleCloudPlatform/data-science-on-gcp/blob/master/09_cloudml/flights_model_tf2.ipynb

    :return:
    """
    from tensorflow.feature_column import (categorical_column_with_hash_bucket,
        numeric_column, embedding_column, bucketized_column, crossed_column, indicator_column)

    if len(cols_ref) <= 1 :
        return Xtrain

    dict_sparse, dict_dense = {}, {}
    for cols_groupname in cols_ref :
        assert cols_groupname in cols_type_received, "Error missing colgroup in config data_pars[cols_model_type] "

        if cols_groupname == "cols_sparse" :
           col_list = cols_type_received[cols_groupname]
           for coli in col_list :
               m_bucket = min(500, int( Xtrain[coli].nunique()) )
               dict_sparse[coli] = categorical_column_with_hash_bucket(coli, hash_bucket_size= m_bucket)

        if cols_groupname == "cols_dense" :
           col_list = cols_type_received[cols_groupname]
           for coli in col_list :
               dict_dense[coli] = numeric_column(coli)

        if cols_groupname == "cols_cross" :
           col_list = cols_type_received[cols_groupname]
           for coli in col_list :
               m_bucketi = min(500, int( Xtrain[coli[0]].nunique()) )
               m_bucketj = min(500, int( Xtrain[coli[1]].nunique()) )
               dict_sparse[coli[0]+"-"+coli[1]] = crossed_column(coli[0], coli[1], m_bucketi * m_bucketj)

        if cols_groupname == "cols_discretize" :
           col_list = cols_type_received[cols_groupname]
           for coli in col_list :
               bucket_list = np.linspace(min, max, 100).tolist()
               dict_sparse[coli +"_bin"] = bucketized_column(numeric_column(coli), bucket_list)


    #### one-hot encode the sparse columns
    dict_sparse = { colname : indicator_column(col)  for colname, col in dict_sparse.items()}

    ### Embed
    dict_embed  = { 'em_{}'.format(colname) : embedding_column(col, 10) for colname, col in dict_sparse.items()}

    dict_dnn    = {**dict_embed,  **dict_dense}
    dict_linear = {**dict_sparse, **dict_dense}

    return (dict_linear, dict_dnn )



def get_dataset_tuple_keras(pattern, batch_size, mode=tf.estimator.ModeKeys.TRAIN, truncate=None):
    """  ACTUAL Data reading :
           Dataframe ---> TF Dataset  --> feed Keras model

    """
    import os, json, math, shutil
    import tensorflow as tf

    DATA_BUCKET = "gs://{}/flights/chapter8/output/".format(BUCKET)
    TRAIN_DATA_PATTERN = DATA_BUCKET + "train*"
    EVAL_DATA_PATTERN = DATA_BUCKET + "test*"

    CSV_COLUMNS  = ('ontime,dep_delay,taxiout,distance,avg_dep_delay,avg_arr_delay' + \
                    ',carrier,dep_lat,dep_lon,arr_lat,arr_lon,origin,dest').split(',')
    LABEL_COLUMN = 'ontime'
    DEFAULTS     = [[0.0],[0.0],[0.0],[0.0],[0.0],[0.0],\
                    ['na'],[0.0],[0.0],[0.0],[0.0],['na'],['na']]

    def pandas_to_dataset(training_df, coly):
        # tf.enable_eager_execution()
        # features = ['feature1', 'feature2', 'feature3']
        print(training_df)
        training_dataset = (
            tf.data.Dataset.from_tensor_slices(
                (
                    tf.cast(training_df[features].values, tf.float32),
                    tf.cast(training_df[coly].values, tf.int32)
                )
            )
        )

        for features_tensor, target_tensor in training_dataset:
            print(f'features:{features_tensor} target:{target_tensor}')
        return training_dataset

    def load_dataset(pattern, batch_size=1):
      return tf.data.experimental.make_csv_dataset(pattern, batch_size, CSV_COLUMNS, DEFAULTS)

    def features_and_labels(features):
      label = features.pop('ontime') # this is what we will train for
      return features, label

    dataset = load_dataset(pattern, batch_size)
    dataset = dataset.map(features_and_labels)

    if mode == tf.estimator.ModeKeys.TRAIN:
        dataset = dataset.shuffle(batch_size*10)
        dataset = dataset.repeat()
        dataset = dataset.prefetch(1)
    if truncate is not None:
        dataset = dataset.take(truncate)
    return dataset


########################################################################################################################
#                                                                                                                      #
#                               Wide Deep Model Implementation with Tensorflow Data and Features                       #
#                                                                                                                      #
########################################################################################################################

import numpy as np
import pandas as pd
import tensorflow as tf
import pathlib
from tensorflow import feature_column
from tensorflow.keras import layers
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt 
tf.compat.v1.enable_eager_execution()

def log(statement):
    print(statement,flush=True)

def ModelCustom(model_pars,data_pars):

    loss       = model_pars['loss']
    optimizer  = model_pars['optimizer']
    metrics    = model_pars['metric']
    dnn_hidden_units = model_pars['hidden_units']

    inputs = data_pars['inputs']
    linear_feature_columns = data_pars['linear_cols']
    dnn_feature_columns = data_pars['dnn_cols']

    deep = tf.keras.layers.DenseFeatures(dnn_feature_columns, name='deep_inputs')(inputs)
    layers = [int(x) for x in dnn_hidden_units.split(',')]

    for layerno, numnodes in enumerate(layers):
        deep = tf.keras.layers.Dense(numnodes, activation='relu', name='dnn_{}'.format(layerno+1))(deep)  
          
    wide = tf.keras.layers.DenseFeatures(linear_feature_columns, name='wide_inputs')(inputs)
    both = tf.keras.layers.concatenate([deep, wide], name='both')
    output = tf.keras.layers.Dense(1, activation='sigmoid', name='pred')(both)
    model = tf.keras.Model(inputs, output)
    model.compile(optimizer=optimizer, loss=loss,  metrics=metrics)
    return model



class Model(object):
    def __init__(self, model_pars=None, data_pars=None):
        self.model_pars, self.data_pars= model_pars, data_pars
        self.history = None
        if model_pars is None:
            self.model = None

        else:
            self.model = ModelCustom(model_pars,data_pars)
            log(self.model.summary())
        


def fit(data_pars, compute_pars, out_pars=None):
    """
    """
    global model, session
    session = None  # Session type for compute

    train_df = data_pars['train']
    if 'val' in data_pars:
        val_df = data_pars['val']
    else:
        val_df = None
        validation_split = 0.2

    if 'epochs' in compute_pars:

        epochs = compute_pars['epochs']
    else:
        epochs = 10

    if 'verbose' in compute_pars:
        verbose = compute_pars['verbose']
    else:
        verbose = 1

    if 'path_checkpoint' in compute_pars:
        path_checkpoint = compute_pars['path_checkpoint']
    else:
        path_checkpoint = 'ztmp_checkpoint/model_.pth'

    #cpars          = copy.deepcopy( compute_pars.get("compute_pars", {}))   ## issue with pickle
    early_stopping = EarlyStopping(monitor='loss', patience=3)
    model_ckpt     = ModelCheckpoint(filepath = path_checkpoint,save_best_only=True, monitor='loss')


    if val_df:
        hist = model.model.fit(train_df,epochs=epochs,verbose=verbose,validation_data=val_df)
    else:
        hist = model.model.fit(train_df,epochs=epochs,verbose=verbose,validation_split=validation_split)
    model.model.history = hist

    return model.model.history


def predict(Xpred=None,data_pars=None, compute_pars=None, out_pars=None):
    if Xpred is None :
        Xpred = data_pars['test']

    ypred2 = model.model.predict(Xpred)

    log2(ypred2)
    ypred = []

    for i in ypred:
        if i >= 0.5:
            ypred.append(1)
        else:
            ypred.append(0)


    ypred_proba = None  ### No proba
    if compute_pars.get("probability", False):
         ypred_proba = ypred2
    return ypred, ypred_proba


def reset():
    global model, session
    model, session = None, None


def save(path=None,filename='Model_Keras'):
    import dill as pickle, copy
    if path != None:
        path = path

    else:
        path = 'savedWeights_Keras_Model'
    model.filename = filename
    model.path = path
    os.makedirs(path, exist_ok=True)
    ### Keras
    model.model.save(f"{path}/{filename}.h5")
    log('Model Saved')


def model_snapshot(self):
    tf.keras.utils.plot_model(self.model, 'model.png', show_shapes=False, rankdir='LR')


class PrepareFeatureColumns:

    def __init__(self,dataframe):
        
        self.df = dataframe
        self.real_columns = {}
        self.sparse_columns = {}
        self.feature_layer_inputs = {}

    def __df_to_dataset(self,dataframe,target,shuffle=True, batch_size=32):

        dataframe = dataframe.copy()
        labels = dataframe.pop(target)
        ds = tf.data.Dataset.from_tensor_slices((dict(dataframe), labels))
        if shuffle:
            ds = ds.shuffle(buffer_size=len(dataframe))
        ds = ds.batch(batch_size)
        return ds

    def splitData(self,test_split=0.1,val_split=0.1):

        train_df,test_df = train_test_split(self.df,test_size=test_split)
        train_df,val_df = train_test_split(train_df,test_size=val_split)

        log('Files Splitted')
        self.train = train_df
        self.val = val_df
        self.test = test_df

    def data_to_tensorflow(self,target,shuffle_train=True,shuffle_test=False,shuffle_val=False,batch_size=32):

        self.train_df_tf = self.__df_to_dataset(self.train,target=target,shuffle=shuffle_train,batch_size=batch_size)
        self.test_df_tf = self.__df_to_dataset(self.test,target=target,shuffle=shuffle_test,batch_size=batch_size)
        self.val_df_tf = self.__df_to_dataset(self.val,target=target,shuffle=shuffle_val,batch_size=batch_size)

        self.exampleData = self.test_df_tf #for visualization purposes
        log('Datasets Converted to Tensorflow')
        return self.train_df_tf,self.test_df_tf,self.val_df_tf

    def numeric_columns(self,columnsName):
        for header in columnsName:
            numeric = feature_column.numeric_column(header)
            self.real_columns[header] = numeric
            self.feature_layer_inputs[header] = tf.keras.Input(shape=(1,), name=header)
        return numeric


    def bucketized_columns(self,columnsBoundaries):
        for key,value in columnsBoundaries.items():
            col = feature_column.numeric_column(key)
            col_buckets = feature_column.bucketized_column(col,boundaries=value)
            self.sparse_columns[key] = col_buckets
        return col_buckets


    def categorical_columns(self,indicator_column_names,output=False):
        for col_name in indicator_column_names:
            categorical_column = feature_column.categorical_column_with_vocabulary_list(col_name, list(self.df[col_name].unique()))
            indicator_column = feature_column.indicator_column(categorical_column)
            self.sparse_columns[col_name] = indicator_column
            self.feature_layer_inputs[col_name] = tf.keras.Input(shape=(1,), name=col_name,dtype=tf.string)
        
        return indicator_column
    
    def hashed_columns(self,hashed_columns_dict):

        for col_name,bucket_size in hashed_columns_dict.items():
            hashedCol = feature_column.categorical_column_with_hash_bucket(col_name, hash_bucket_size=bucket_size)
            hashedFeature = feature_column.indicator_column(hashedCol)
            self.sparse_columns[col_name] = hashedFeature

        return hashedFeature

    def crossed_feature_columns(self,columns_crossed,nameOfLayer,bucket_size=10):
        crossed_feature = feature_column.crossed_column(columns_crossed, hash_bucket_size=bucket_size)
        crossed_feature = feature_column.indicator_column(crossed_feature)
        self.sparse_columns[nameOfLayer] = crossed_feature

        return crossed_feature

    def embeddings_columns(self,columnsname):

        for col_name,dimension in columnsname.items():
            embCol = feature_column.categorical_column_with_vocabulary_list(col_name, self.df[col_name].unique())
            embedding = feature_column.embedding_column(embCol, dimension=dimension)
            self.real_columns[col_name] = embedding

        return embedding

    def transform_output(self,featureColumn):

        feature_layer = layers.DenseFeatures(featureColumn)
        example = next(iter(self.exampleData))[0]
        log(feature_layer(example).numpy())

    
    def get_features(self):
        return self.real_columns,self.sparse_columns,self.feature_layer_inputs








def test(config=''):
    """
        Group of columns for the input model
           cols_input_group = [ ]
          for cols in cols_input_group,

    :param config:
    :return:
    """

    dataset_url = 'http://storage.googleapis.com/download.tensorflow.org/data/petfinder-mini.zip'
    csv_file = 'datasets/petfinder-mini/petfinder-mini.csv'

    tf.keras.utils.get_file('petfinder_mini.zip', dataset_url,
                            extract=True, cache_dir='.')

    print('Data Frame Loaded')
    dataframe = pd.read_csv(csv_file)
    #X_train_full, X_test, y_train_full, y_test = train_test_split(X, y, random_state=2021, stratify=y)
    #X_train, X_valid, y_train, y_valid         = train_test_split(X_train_full, y_train_full, random_state=2021, stratify=y_train_full)
    dataframe['target'] = np.where(dataframe['AdoptionSpeed']==4, 0, 1)

    # Drop un-used columns.
    dataframe = dataframe.drop(columns=['AdoptionSpeed', 'Description'])
    prepare = PrepareFeatureColumns(dataframe)

    prepare.splitData()
    train_df,test_df,val_df = prepare.data_to_tensorflow(target='target')

    #Numeric Columns creation
    numeric_columns = ['PhotoAmt', 'Fee', 'Age']
    prepare.numeric_columns(numeric_columns)

    #Categorical Columns
    indicator_column_names = ['Type', 'Color1', 'Color2', 'Gender', 'MaturitySize',
                          'FurLength', 'Vaccinated', 'Sterilized', 'Health','Breed1']

    prepare.categorical_columns(indicator_column_names)
    
    #Bucketized Columns
    bucket_cols = {
        'Age': [1,2,3,4,5]
    }
    prepare.bucketized_columns(bucket_cols)

    

    #Embedding Columns
    embeddingCol = {
        'Breed1':8
    }
    
    linear,dnn,inputs = prepare.get_features()
    
    model_pars = {
        'loss' : 'binary_crossentropy',
        'optimizer':'adam',
        'metric': ['accuracy'],
        'hidden_units': '64,32,16'
    }

    data_pars = {
        'inputs': inputs,
        'linear_cols': linear.values(),
        'dnn_cols': dnn.values(),
        'train': train_df,
        'test': test_df,
        'val': val_df
    }

    compute_pars = {
        'epochs':2,
        'verbose': 1,
        'path_checkpoint': 'checkpoint/model.pth'
    }


    ######## Run ###########################################
    test_helper(model_pars, data_pars, compute_pars)

def test_helper(model_pars, data_pars, compute_pars):
    global model, session
    root  = "ztmp/"
    model = Model(model_pars=model_pars, data_pars=data_pars)

    log('\n\nTraining the model..')
    model.fit(data_pars=data_pars, compute_pars=compute_pars)

    log('Predict data..')
    ypred, ypred_proba = model.predict(data_pars=data_pars)
    log(f'Top 5 y_pred: {np.squeeze(ypred)[:5]}')


    log('Saving model..')
    model.save(path= root + '/model_dir/')

    log('Model architecture:')
    log(model.model.summary())

    log('Model Snapshot')
    model.model_snapshot()

####################################################################################################################
if __name__ == '__main__':
    import fire
    fire.Fire(test)










def get_dataset2(data_pars=None, task_type="train", **kw):
    """
      return tuple of Tensoflow
    """
    # log(data_pars)
    data_type = data_pars.get('type', 'ram')
    cols_ref  = cols_ref_formodel

    if data_type == "ram":
        # cols_ref_formodel = ['cols_cross_input', 'cols_deep_input', 'cols_deep_input' ]
        ### dict  colgroup ---> list of colname
        cols_type_received     = data_pars.get('cols_model_type2', {} )  ##3 Sparse, Continuous

        if task_type == "predict":
            d = data_pars[task_type]
            Xtrain       = d["X"]
            Xtuple_train = get_dataset_tuple_keras(Xtrain, cols_type_received, cols_ref)
            return Xtuple_train

        if task_type == "eval":
            d = data_pars[task_type]
            Xtrain, ytrain  = d["X"], d["y"]
            Xtuple_train    = get_dataset_tuple_keras(Xtrain, cols_type_received, cols_ref)
            return Xytuple_train

        if task_type == "train":
            d = data_pars[task_type]
            Xtrain, ytrain, Xtest, ytest  = d["Xtrain"], d["ytrain"], d["Xtest"], d["ytest"]

            ### dict  colgroup ---> list of df
            Xytuple_train = get_dataset_tuple_keras(Xtrain, ytrain, cols_type_received, cols_ref)
            Xytuple_test  = get_dataset_tuple_keras(Xtest, ytest, cols_type_received, cols_ref)

            log2("Xtuple_train", Xytuple_train)

            return Xytuple_train, Xytuple_test


    elif data_type == "file":
        raise Exception(f' {data_type} data_type Not implemented ')

    raise Exception(f' Requires  Xtrain", "Xtest", "ytrain", "ytest" ')

def Modelsparse2():
    """
    https://github.com/GoogleCloudPlatform/data-science-on-gcp/blob/master/09_cloudml/flights_model_tf2.ipynb

    :return:
    """
    import tensorflow as tf
    NBUCKETS = 10

    real = {
        colname : tf.feature_column.numeric_column(colname)
              for colname in
                ('dep_delay,taxiout,distance,avg_dep_delay,avg_arr_delay' +
                 ',dep_lat,dep_lon,arr_lat,arr_lon').split(',')
    }
    inputs = {
        colname : tf.keras.layers.Input(name=colname, shape=(), dtype='float32')
              for colname in real.keys()
    }

    sparse = {
          'carrier': tf.feature_column.categorical_column_with_vocabulary_list('carrier',
                      vocabulary_list='AS,VX,F9,UA,US,WN,HA,EV,MQ,DL,OO,B6,NK,AA'.split(',')),
          'origin' : tf.feature_column.categorical_column_with_hash_bucket('origin', hash_bucket_size=1000),
          'dest'   : tf.feature_column.categorical_column_with_hash_bucket('dest', hash_bucket_size=1000)
    }
    inputs.update({
        colname : tf.keras.layers.Input(name=colname, shape=(), dtype='string')
              for colname in sparse.keys()
    })



    latbuckets = np.linspace(20.0, 50.0, NBUCKETS).tolist()  # USA
    lonbuckets = np.linspace(-120.0, -70.0, NBUCKETS).tolist() # USA
    disc = {}
    disc.update({
           'd_{}'.format(key) : tf.feature_column.bucketized_column(real[key], latbuckets)
              for key in ['dep_lat', 'arr_lat']
    })
    disc.update({
           'd_{}'.format(key) : tf.feature_column.bucketized_column(real[key], lonbuckets)
              for key in ['dep_lon', 'arr_lon']
    })

    # cross columns that make sense in combination
    sparse['dep_loc'] = tf.feature_column.crossed_column([disc['d_dep_lat'], disc['d_dep_lon']], NBUCKETS*NBUCKETS)
    sparse['arr_loc'] = tf.feature_column.crossed_column([disc['d_arr_lat'], disc['d_arr_lon']], NBUCKETS*NBUCKETS)
    sparse['dep_arr'] = tf.feature_column.crossed_column([sparse['dep_loc'], sparse['arr_loc']], NBUCKETS ** 4)
    #sparse['ori_dest'] = tf.feature_column.crossed_column(['origin', 'dest'], hash_bucket_size=1000)

    # embed all the sparse columns
    embed = {
           'embed_{}'.format(colname) : tf.feature_column.embedding_column(col, 10)
              for colname, col in sparse.items()
    }
    real.update(embed)

    # one-hot encode the sparse columns
    sparse = {
        colname : tf.feature_column.indicator_column(col)
              for colname, col in sparse.items()
    }


    print(sparse.keys())
    print(real.keys())


    # Build a wide-and-deep model.
    def wide_and_deep_classifier(inputs, linear_feature_columns, dnn_feature_columns, dnn_hidden_units):
        deep = tf.keras.layers.DenseFeatures(dnn_feature_columns, name='deep_inputs')(inputs)
        layers = dnn_hidden_units
        for layerno, numnodes in enumerate(layers):
            deep = tf.keras.layers.Dense(numnodes, activation='relu', name='dnn_{}'.format(layerno+1))(deep)
        wide = tf.keras.layers.DenseFeatures(linear_feature_columns, name='wide_inputs')(inputs)
        both = tf.keras.layers.concatenate([deep, wide], name='both')
        output = tf.keras.layers.Dense(1, activation='sigmoid', name='pred')(both)
        model = tf.keras.Model(inputs, output)
        model.compile(optimizer='adam',
                      loss='binary_crossentropy',
                      metrics=['accuracy'])
        return model


    DNN_HIDDEN_UNITS = 10
    model = wide_and_deep_classifier(
        inputs,
        linear_feature_columns = sparse.values(),
        dnn_feature_columns = real.values(),
        dnn_hidden_units = DNN_HIDDEN_UNITS)
    tf.keras.utils.plot_model(model, 'flights_model.png', show_shapes=False, rankdir='LR')

