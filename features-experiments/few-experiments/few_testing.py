import sys
import pandas as pd
import numpy as np
import few


if __name__ == '__main__':
    """ Launches FEW experiments on the dataset given as cli arguments
        Usage : python few_testing.py path_to_train_dataset path_to_validation_dataset path_to_parameters
        Example : python few_testing.py ../../data/input/jane/train/train01 ../../data/input/jane/train/train02 ../genetic_algo_params.txt
    """
    train_dataset_path = sys.argv[1]
    validation_dataset_path = sys.argv[2]
    params = pd.read_csv(sys.argv[3])

    print("Loading training dataset")
    dfTrain = pd.read_csv(train_dataset_path)
    print("Loading validation dataset")
    dfValidate = pd.read_csv(validation_dataset_path)

    print("Converting training dataframe to numpy")
    X_train = dfTrain.to_numpy()
    print("Computing training class label")
    y_train = ((dfTrain["resp"] * dfTrain['weight']) > 0).astype(np.int)

    X_train = X_train[0:10000]
    y_train = y_train[0:10000]

    print("Converting validation dataframe to numpy")
    X_validate = dfValidate.to_numpy()
    print("Computing validation class label")
    y_validate = ((dfValidate["resp"] * dfValidate['weight']) > 0).astype(np.int)


    for i in range(params.shape[0]):
        cur_param = params.iloc[i]
        print("="*70)
        print("Current parameters used :")
        print(cur_param)
        print("*"*40)
        print("Instantiating FEW engine")
        _few = few.FEW(random_state=5577,
                      verbosity=1,
                      generations=cur_param.generations.astype(int),
                      population_size=cur_param.population_size.astype(int),
                      mutation_rate=cur_param.mutation_rate,
                      crossover_rate=cur_param.crossover_rate)

        print("FEW fitting")
        _few.fit(X_train, y_train)

        print('\nTraining accuracy: {}'.format(_few.score(X_train, y_train)))
        print('Validation accuracy: {}'.format(_few.score(X_validate, y_validate)))
        print('\Model: {}'.format(_few.print_model()))
