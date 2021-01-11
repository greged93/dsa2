# pylint: disable=C0321,C0103,E1221,C0301,E1305,E1121,C0302,C0330
# -*- coding: utf-8 -*-
"""
Run template


python run.py data_profile --config_uri titanic_classifier.py::titanic_lightgbm

                                                                                     
python run.py preprocess --config_uri titanic_classifier.py::titanic_lightgbm


python run.py train --config_uri titanic_classifier.py::titanic_lightgbm


python run.py predict --config_uri titanic_classifier.py::titanic_lightgbm



"""
import warnings, copy
warnings.filterwarnings('ignore')
import os, sys
import pandas as pd

####################################################################################
from source.util_feature import log


###### Path ########################################################################
print( os.getcwd())
root = os.path.abspath(os.getcwd()).replace("\\", "/") + "/"
print(root)

dir_data  = os.path.abspath( root + "/data/" ) + "/"
dir_data  = dir_data.replace("\\", "/")
print(dir_data)



def get_global_pars(config_uri=""):
  log("#### Model Params Dynamic loading  ###############################################")
  from source.util_feature import load_function_uri
  model_dict_fun = load_function_uri(uri_name=config_uri )

  #### Get dict + Update Global variables
  model_dict     = model_dict_fun()   ### params
  return model_dict





####################################################################################################
########## Init variable ###########################################################################
# globals()[config_name]()


###################################################################################
########## Profile data #############################################################
def data_profile(config_uri='titanic_classifier.py::titanic_lightgbm'):
    from source.run_feature_profile import run_profile
    mdict = get_global_pars( root + config_uri)
    m = mdict['global_pars']
    log(mdict)

    run_profile(path_data   = m['path_data_train'],
               path_output  = m['path_model'] + "/profile/",  
               n_sample     = 5000,
              ) 


###################################################################################
########## Preprocess #############################################################
def preprocess(config_uri='titanic_classifier.py::titanic_lightgbm'):
    from source import run_preprocess
    mdict = get_global_pars( root + config_uri)
    m = mdict['global_pars']
    log(mdict)

    run_preprocess.run_preprocess(model_name        =  m['config_name'], 
                                  path_data         =  m['path_data_train'], 
                                  path_output       =  m['path_model'], 
                                  path_config_model =  m['path_config_model'], 
                                  n_sample          =  m['n_sample'],
                                  mode              =  'run_preprocess')


############################################################################
## Feature engineering using genetic algorithm
def genetic_algo(config_uri='jane.py::jane_lightgbm'):

    from source import run_geneticalgo
    mdict = get_global_pars( root + config_uri)
    m = mdict['global_pars']
    log(m)
    
    run_geneticalgo.run_geneticalgo(path_data  = m['path_data_train'],
                                    path_param = m['ga_param_file'],
                                    gene_algo  = m['gene_algo'],
                                    col_delete = mdict['data_pars']['cols_input_type']['coldelete'])
    

############################################################################
########## Train ###########################################################
def train(config_uri='titanic_classifier.py::titanic_lightgbm'):
    from source import run_train
    mdict = get_global_pars( root + config_uri)
    m = mdict['global_pars']
    log(mdict)

    run_train.run_train(config_model_name =  m['config_name'],
                        path_data         =  m['path_data_train'],
                        path_output       =  m['path_model'],
                        path_config_model =  m['path_config_model'] , 
                        n_sample = m['n_sample']
                        )



######### Check model #############################################################
def check(config_uri='titanic_classifier.py::titanic_lightgbm'):
    mdict = get_global_pars( root + config_uri)
    m = mdict['global_pars']
    log(mdict)
    pass








########################################################################################
####### Inference ######################################################################
def predict(config_uri='titanic_classifier.py::titanic_lightgbm'):
    from source import run_inference
    mdict = get_global_pars( root + config_uri)
    m = mdict['global_pars']
    log(mdict)

    run_inference.run_predict(model_name  = m['model_name'],
                              path_model  = m['path_model'],
                              path_data   = m['path_data_test'],
                              path_output = m['path_output_pred'],
                              n_sample    = m['n_sample']
                              )


def run_all(config_uri='titanic_classifier.py::titanic_lightgbm'):
    preprocess(config_uri)
    train(config_uri)
    check(config_uri)
    predict(config_uri)




###########################################################################################################
###########################################################################################################
if __name__ == "__main__":
    import fire
    fire.Fire()
    
