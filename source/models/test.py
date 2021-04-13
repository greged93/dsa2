
from petastorm.tf_utils import make_petastorm_dataset
import petastorm
import os
import tensorflow as tf 
from petastorm import make_batch_reader
import numpy as np

def pack_features_vector(features, labels):
    """Pack the features into a single array."""

    #print(f'Features: {features}')
    print(dir(features))
    features = tf.stack(list(features), axis=1)
    return features, labels

def get_dataset_split_for_model_petastorm(Xtrain, ytrain=None, pars:dict=None):
    """  Split data for moel input/
    Xtrain  ---> Split INTO  tuple PetaStorm Reader
    https://github.com/uber/petastorm/blob/master/petastorm/reader.py#L61-L134
    :param Xtrain:  path
    :param cols_type_received:
    :param cols_ref:
    :return:
    """
    file = r'C:\Users\TusharGoel\Desktop\Upwork\project4\dsa2\datasets\parquet\f01.parquet'
    dataset_url_train = Xtrain
    all_cols = 'colnum_0,colnum_1,colnum_2,colnum_3,colnum_4,colnum_5,colnum_6,colnum_7,colnum_8,colnum_9,colnum_10,colcat_1'
    all_cols = all_cols.split(',')
    label = 'y'
    batch_size = 128
    num_classes = 2
    epochs = 12
    file_path = '/C:/Users/TusharGoel/Desktop/Upwork/project4/dsa2/'+ dataset_url_train
    file = "file://" + file_path
    BATCH_SIZE = 32
    train_reader = make_batch_reader(file)
    #yield tensor
    train_ds = make_petastorm_dataset(train_reader) \
            .apply(tf.data.experimental.unbatch()) \
            .batch(BATCH_SIZE) \
            .map(lambda x: [tf.reshape(list(getattr(x, col) for col in all_cols),[-1,12]),tf.reshape(x.y,[-1,1])])
    #train_ds = train_ds.map(pack_features_vector)
    train_ds = train_ds.make_one_shot_iterator()
    #print(f'Train Dataset: {train_ds}')
    
    tensor = np.array(train_ds.get_next())
    print(tensor)
    return tensor   
    #print(train_ds)   
        

    '''train_dataset  = make_petastorm_dataset(reader)
        #trai
    
    

    ### Re-shape  #############################################
    #train_dataset = train_dataset.map(lambda x: (tf.reshape(x.image, (28, 28, 1)), tf.reshape(x.digit, [1])))

    #print(dir(train_dataset))
    
    train_dataset = train_dataset.map(lambda x: (tf.reshape(x,[-1,1]),tf.reshape(getattr(x,label),[-1,1])))
    train_dataset = train_dataset.map(pack_features_vector)'''
    ###########################################################
    #train_dataset = train_dataset.batch(batch_size, drop_remainder=True)
    

tensor = get_dataset_split_for_model_petastorm('datasets/parquet/f01.parquet')

from tensorflow.keras import layers
model = tf.keras.Sequential([
    layers.Dense(32, activation='elu'),
    layers.Dense(32, activation='elu'),
    layers.Dense(1,activation='sigmoid') 
    ])


model.compile(optimizer='adam',
            loss=tf.keras.losses.BinaryCrossentropy(from_logits=True),
            metrics=['accuracy'])    
model.fit(tensor,
        batch_size=32,
        epochs=30,
        verbose=1
        )
