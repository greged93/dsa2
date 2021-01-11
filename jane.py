# pylint: disable=C0321,C0103,E1221,C0301,E1305,E1121,C0302,C0330
# -*- coding: utf-8 -*-
"""
You can put hardcode here, specific to titatinic dataet
All in one file config
!  python jane_classifier.py  train
!  python jane_classifier.py  check
!  python jane_classifier.py  predict
"""
import warnings, copy
warnings.filterwarnings('ignore')
import os, sys
import pandas as pd, numpy as np
####################################################################################
from source import util_feature

from source.util_feature import   log


###### Path ########################################################################
print( os.getcwd())
root = os.path.abspath(os.getcwd()).replace("\\", "/") + "/"
print(root)

dir_data  = os.path.abspath( root + "/data/" ) + "/"
dir_data  = dir_data.replace("\\", "/")
print(dir_data)




####################################################################################
config_file  = "jane.py"   ### name of file which contains data configuration
data_name    = "jane"     ### in data/input/



path_data   = root + f'data/input/{data_name}/'



config_name  = 'jane_lightgbm'   ### name of function which contains data configuration 
n_sample     = 200


cols_all = ['date', 'weight', 'resp_1', 'resp_2', 'resp_3', 'resp_4', 'resp', 'feature_0', 'feature_1', 'feature_2', 'feature_3', 'feature_4', 'feature_5', 'feature_6', 'feature_7', 'feature_8', 'feature_9', 'feature_10', 'feature_11', 'feature_12', 'feature_13', 'feature_14', 'feature_15', 'feature_16', 'feature_17', 'feature_18', 'feature_19', 'feature_20', 'feature_21', 'feature_22', 'feature_23', 'feature_24', 'feature_25', 'feature_26', 'feature_27', 'feature_28', 'feature_29', 'feature_30', 'feature_31', 'feature_32', 'feature_33', 'feature_34', 'feature_35', 'feature_36', 'feature_37', 'feature_38', 'feature_39', 'feature_40', 'feature_41', 'feature_42', 'feature_43', 'feature_44', 'feature_45', 'feature_46', 'feature_47', 'feature_48', 'feature_49', 'feature_50', 'feature_51', 'feature_52', 'feature_53', 'feature_54', 'feature_55', 'feature_56', 'feature_57', 'feature_58', 'feature_59', 'feature_60', 'feature_61', 'feature_62', 'feature_63', 'feature_64', 'feature_65', 'feature_66', 'feature_67', 'feature_68', 'feature_69', 'feature_70', 'feature_71', 'feature_72', 'feature_73', 'feature_74', 'feature_75', 'feature_76', 'feature_77', 'feature_78', 'feature_79', 'feature_80', 'feature_81', 'feature_82', 'feature_83', 'feature_84', 'feature_85', 'feature_86', 'feature_87', 'feature_88', 'feature_89', 'feature_90', 'feature_91', 'feature_92', 'feature_93', 'feature_94', 'feature_95', 'feature_96', 'feature_97', 'feature_98', 'feature_99', 'feature_100', 'feature_101', 'feature_102', 'feature_103', 'feature_104', 'feature_105', 'feature_106', 'feature_107', 'feature_108', 'feature_109', 'feature_110', 'feature_111', 'feature_112', 'feature_113', 'feature_114', 'feature_115', 'feature_116', 'feature_117', 'feature_118', 'feature_119', 'feature_120', 'feature_121', 'feature_122', 'feature_123', 'feature_124', 'feature_125', 'feature_126', 'feature_127', 'feature_128', 'feature_129', 'ts_id']


##### Xinput
colid   = 'ts_id'
coldate = ['date']
colcat = ['feature_0']

colnum = ['weight', 'feature_1', 'feature_2', 'feature_3', 'feature_4', 'feature_5',
          'feature_6', 'feature_7', 'feature_8', 'feature_9', 'feature_10', 'feature_11', 'feature_12', 'feature_13', 'feature_14', 'feature_15', 'feature_16', 'feature_17', 'feature_18', 'feature_19', 'feature_20', 'feature_21', 'feature_22', 'feature_23', 'feature_24', 'feature_25', 'feature_26', 'feature_27', 'feature_28', 'feature_29', 'feature_30', 'feature_31', 'feature_32', 'feature_33', 'feature_34', 'feature_35', 'feature_36', 'feature_37', 'feature_38', 'feature_39', 'feature_40', 'feature_41', 'feature_42', 'feature_43', 'feature_44', 'feature_45', 'feature_46', 'feature_47', 'feature_48', 'feature_49', 'feature_50', 'feature_51', 'feature_52', 'feature_53', 'feature_54', 'feature_55', 'feature_56', 'feature_57', 'feature_58', 'feature_59', 'feature_60', 'feature_61', 'feature_62', 'feature_63', 'feature_64', 'feature_65', 'feature_66', 'feature_67', 'feature_68', 'feature_69', 'feature_70', 'feature_71', 'feature_72', 'feature_73', 'feature_74', 'feature_75', 'feature_76', 'feature_77', 'feature_78', 'feature_79', 'feature_80', 'feature_81', 'feature_82', 'feature_83', 'feature_84', 'feature_85', 'feature_86', 'feature_87', 'feature_88', 'feature_89', 'feature_90', 'feature_91', 'feature_92', 'feature_93', 'feature_94', 'feature_95', 'feature_96', 'feature_97', 'feature_98', 'feature_99', 'feature_100', 'feature_101', 'feature_102', 'feature_103', 'feature_104', 'feature_105', 'feature_106', 'feature_107', 'feature_108', 'feature_109', 'feature_110', 'feature_111', 'feature_112', 'feature_113', 'feature_114', 'feature_115', 'feature_116', 'feature_117', 'feature_118', 'feature_119', 'feature_120', 'feature_121', 'feature_122', 'feature_123', 'feature_124', 'feature_125', 'feature_126', 'feature_127', 'feature_128', 'feature_129' ]


colcross= []

coldelete = ["Unnamed: 0", "action", "date", "weight", "resp_1", "resp_2", "resp_3", "resp_4", "resp"]

####  used to evaluate action  ###################################
coly2 =  [  'resp_1', 'resp_2', 'resp_3', 'resp_4', 'resp', ]
coly   =   ['action']   ## resp >0 :  1  else 0



######################################################################################
#### Create label  coly = action
def label_calc(df):
  coly2 =  [  'resp_1', 'resp_2', 'resp_3', 'resp_4', 'resp', ]
  def get_binary(x) :
      if x['resp'] > 0.01 : return 1
      else : return 0

  df['y'] = df[ coly2].apply(lambda  x : get_binary(x), axis=1)
  return df[['y'] + coly2 ]



def utility_calc(p, i):
    t = (p.sum() / np.sqrt((p*p).sum()) ) * np.sqrt((250/i))
    u = min(max(t, 0), 6) * p.sum()
    return u


def generate_label(path_features="train00", path_label=""):
    df  = pd.read_parquet( path_data + "/" + path_features + "/features.parquet")
    df  = df.set_index(colid)
    df  = df[coly2]
    dfy = label_calc(df)
    dfy.to_parquet(   path_data + "/" + path_label + "/label.parquet" )
    log(dfy)
    log( path_data + "/" + path_label + "/label.parquet")





####################################################################################
##### Params########################################################################
def jane_lightgbm(path_model_out="") :
    """
       Contains all needed informations for Light GBM Classifier model, used for jane classification task
    """
    global path_config_model, path_model, path_data_train, path_data_test, path_output_pred, n_sample, model_name, ga_param_file, gene_algo

    config_name       = 'jane_lightgbm'
    model_name        = 'LGBMClassifier'

    path_config_model = root + f"/{config_file}"
    path_model        = f'data/output/{data_name}/a01_{model_name}/'
    path_data_train   = f'data/input/{data_name}/train/'
    path_data_test    = f'data/input/{data_name}/test/'
    path_output_pred  = f'/data/output/{data_name}/pred_a01_{config_name}/'
    
    ga_param_file     = 'config/ga_params.txt'
    gene_algo         = 'few'
    
    n_sample          = 1000


    def post_process_fun(y):
        ### After prediction is done
        return  y.astype('int')

    def pre_process_fun(y):
        ### Before the prediction is done
        return  y.astype('int')

    model_dict = {'model_pars': {
        'model_path'       : path_model_out

        ### LightGBM API model       ###################################
        ,'config_model_name': model_name    ## ACTUAL Class name for model_sklearn.py
        ,'model_pars'       : {'objective': 'binary',
                                'learning_rate':0.03,'boosting_type':'gbdt'    ### Model hyperparameters


                               }

        ### After prediction  ##########################################
        , 'post_process_fun' : post_process_fun
        , 'pre_process_pars' : {'y_norm_fun' :  None ,
                                
                                ### Pipeline for data processing.
                               'pipe_list'  : [ 'filter',     ### Fitler the data
                                                'label',      ### Normalize the label
                                                # 'dfnum_bin',  ### Create bins for numerical columns
                                                # 'dfnum_hot',  ### One hot encoding for numerical columns
                                                'dfcat_bin',  ### Create bins for categorical columns
                                                #'dfcat_hot',  ### One hot encoding for categorical columns
                                                # 'dfcross_hot', 
                                            ]   ### Crossing of features which are one hot encoded
                               }
        },
      'compute_pars': { 'metric_list': ['accuracy_score',]
                      },

      'data_pars': {
          'cols_input_type' : {
                     "coly"   :   "action"
                    ,"colid"  :   colid
                    ,"colcat" :   colcat
                    ,"colnum" :   colnum
                    ,"coltext" :  []
                    ,"coldate" :  coldate
                    ,"colcross" : colcross
                    ,"coldelete" : coldelete
                   },

          ### used for the model input
           ### Defines features columns of the dataset, and it is highly dependant with pipe_list
          # cols['cols_model'] = cols["colnum"] + cols["colcat_bin"]  # + cols[ "colcross_onehot"]
          'cols_model_group': [ 'colnum', 'colcat_bin']


          ### Actual column namaes to be filled automatically  from cols_model_group
         ,'cols_model':       []      # cols['colcat_model'],   
         ,'coly':             []      # cols['coly']


          ### Filter data rows
         ,'filter_pars': { 'ymax' : 2 ,'ymin' : -1 }

         }

     ,'global_pars' : {}
      }

    global_pars = [ 'config_name', 'model_name', 'path_config_model', 'path_model', 'path_data_train', 
              'path_data_test', 'path_output_pred', 'n_sample', 'ga_param_file', 'gene_algo'
            ]
    for t in global_pars:
      model_dict['global_pars'][t] = globals()[t] 


    return model_dict






####################################################################################################
########## Init variable ###########################################################################
globals()[config_name]()   






import run
###################################################################################
########## Preprocess #############################################################
def preprocess():
    """
    Preprocessing of input data, in order to prepare them for training

    """
    import run
    run.preprocess(config_uri = config_file + '::' + config_name)

###################################################################################
########## Feature Engineering ####################################################
def feat_engi():
    """
    Perform feature engineerring using genetic algorithms
    """
    import run
    run.genetic_algo(config_uri = config_file + '::' + config_name)

########## Train ###########################################################
def train():
    """
    Splits preprocessed data into train and test, and fits them in the model

    """
    import run
    run.train(config_uri = config_file + '::' + config_name)

######### Check model #############################################################
def check():
    """
    It runs trained model and gives feature imporance graph as ouput
    """
    import run
    run.check(config_uri = config_file + '::' + config_name)


    #! python source/run_inference.py  run_predict  --config_model_name  LGBMRegressor  --n_sample 1000   --path_model /data/output/a01_lightgbm_huber/    --path_output /data/output/pred_a01_lightgbm_huber/    --path_data /data/input/train/


####### Inference ######################################################################
def predict():
    """ Creates csv file with predictions
    """
    run.predict(config_uri = config_file + '::' + config_name)


def run_all():
    """ Function which runs all previous functions, in order to perform all tasks, from preprocessing of data to final prediction
    """
    preprocess()
    feat_engi()
    train()
    check()
    predict()



###########################################################################################################
###########################################################################################################
"""
python  jane.py  data_profile
python  jane.py  preprocess
python  jane.py  feat_engi
python  jane.py  train
python  jane.py  check
python  jane.py  predict
python  jane.py  run_all
"""
if __name__ == "__main__":
    import fire
    fire.Fire()
    
