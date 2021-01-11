import sys
import pandas as pd
import numpy as np
import few
from sklearn import datasets as skdt

if __name__ == '__main__':
    """ Launches FEW experiments on sklearn dataset to see how it works  
    """

    if sys.argv[1] == "boston":
        dataset = skdt.load_boston()
    elif sys.argv[1] == "iris":
        dataset = skdt.load_iris()
    elif sys.argv[1] == "diabetes":
        dataset = skdt.load_diabetes()
    elif sys.argv[1] == "wine":
        dataset = skdt.load_wine()
    elif sys.argv[1] == "cancer":
        dataset = skdt.load_breast_cancer()
    else:
        print("Loading cancer as default")
        dataset = skdt.load_breast_cancer()

    X = dataset['data']
    # X = np.nan_to_num(X)
    y = dataset['target']
    dataset_length = len(X)
    feature_number = len(dataset.feature_names)



    print("Splitting dataset in train(60%)/validation(20%)/test(20%)")
    train_idx = int(dataset_length * 0.6)
    validate_idx = int(dataset_length * 0.8)

    X_train = X[:train_idx]
    y_train = y[:train_idx]

    X_validate = X[train_idx:validate_idx]
    y_validate = y[train_idx:validate_idx]

    X_test = X[validate_idx:]
    y_test = y[validate_idx:]

    print("Instantiating FEW engine")
    few = few.FEW(random_state=10, verbosity=1)
    print("FEW fitting")
    few.fit(X_train, y_train)

    print('\nTraining accuracy: {}'.format(few.score(X_train, y_train)))
    print('Validation accuracy: {}'.format(few.score(X_validate, y_validate)))
    print('\Model: {}'.format(few.print_model()))


