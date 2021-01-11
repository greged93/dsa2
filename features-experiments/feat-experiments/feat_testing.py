import sys
import numpy as np
import pandas as pd
from feat import Feat

if __name__ == '__main__':
    """ Launches Feat experiments on the dataset given as cli arguments
        Usage : python feat_testing.py path_to_train_dataset path_to_validation_dataset path_to_parameters verbosity
        Example : python feat_testing.py ../../data/input/jane/train/train01 ../../data/input/jane/train/train02 ../genetic_algo_params.txt 1
    """
    train_dataset_path = sys.argv[1]
    validation_dataset_path = sys.argv[2]
    params = pd.read_csv(sys.argv[3])
    try:
        verbose = int(sys.argv[4])
    except IndexError:
        verbose = 2



    dfTrain = pd.read_csv(train_dataset_path)
    dfValidate = pd.read_csv(validation_dataset_path)

    X_train = dfTrain.to_numpy()
    y_train = ((dfTrain["resp"] * dfTrain['weight']) > 0).astype(np.int)

    for i in range(params.shape[0]):
        cur_param = params.iloc[i]
        print("=" * 70)
        print("Current parameters used :")
        print(cur_param)
        print("*" * 40)

        est = Feat(gens=cur_param.generations.astype(int),
                   pop_size=cur_param.population_size.astype(int),
                   cross_rate=cur_param.crossover_rate,
                   verbosity=verbose)
        est.fit(X_train, y_train)
