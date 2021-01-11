# -*- coding: utf-8 -*- 
import warnings
warnings.filterwarnings('ignore')


import sys
import numpy as np
import pandas as pd


def log(*s, n=0, m=1):
    sspace = "#" * n
    sjump = "\n" * m

    ### Implement Logging
    print(sjump, sspace, s, sspace, flush=True)


def run_few(train_input, col_delete, parameters, verbosity):
    import few

    train_dataset_path = train_input + "train01.zip"
    params = pd.read_csv(parameters)
    verbose = verbosity

    dfTrain = pd.read_csv(train_dataset_path)

    y_train = ((dfTrain["resp"] * dfTrain['weight']) > 0).astype(np.int)
    dfTrain = dfTrain.drop(columns=col_delete)
    X_train = dfTrain.to_numpy()


    for i in range(params.shape[0]):
        cur_param = params.iloc[i]
        _few = few.FEW(random_state=5577,
                      verbosity=verbose,
                      generations=cur_param.generations.astype(int),
                      population_size=cur_param.population_size.astype(int),
                      mutation_rate=cur_param.mutation_rate,
                      crossover_rate=cur_param.crossover_rate)

        _few.fit(X_train, y_train)
        log('\Model: {}'.format(_few.print_model()))


    return


def run_feat(train_input, trim_columns, parameters, verbosity):
    from feat import Feat

    train_dataset_path = train_input + "train01.zip"
    params = pd.read_csv(parameters)
    verbose = verbosity

    dfTrain = pd.read_csv(train_dataset_path)
    y_train = ((dfTrain["resp"] * dfTrain['weight']) > 0).astype(np.int)
    dfTrain = dfTrain.drop(columns=trim_columns)
    X_train = dfTrain.to_numpy()


    for i in range(params.shape[0]):
        cur_param = params.iloc[i]
        est = Feat(gens=cur_param.generations.astype(int),
                   pop_size=cur_param.population_size.astype(int),
                   cross_rate=cur_param.crossover_rate,
                   ml='LR',
                   classification=True,
                   verbosity=verbose)
        est.fit(X_train, y_train)
    	
    return


def run_geneticalgo(path_data, path_param, gene_algo, col_delete):
    # model_dict_fun = load_function_uri(uri_name=path_config_model + "::" + config_model_name)
    # model_dict     = model_dict_fun(path_model_out)

    if gene_algo == "few":
        run_few(path_data, col_delete, path_param, 1)
    elif gene_algo == "feat":
        run_feat(path_data, col_delete, path_param, 1)
    else:
        log("No algorithm chosen for genetic transformations of features. Exiting...")
    
    return
    
