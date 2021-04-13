'''from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split

import matplotlib.pyplot as plt
import seaborn as sb

from pmlb import fetch_data, classification_dataset_names

logit_test_scores = []
gnb_test_scores = []

for classification_dataset in classification_dataset_names[:5]:
    X, y = fetch_data(classification_dataset, return_X_y=True)
    train_X, test_X, train_y, test_y = train_test_split(X, y)

    logit = SVC()
    gnb = GaussianNB()

    logit.fit(train_X, train_y)
    gnb.fit(train_X, train_y)

    logit_test_scores.append(logit.score(test_X, test_y))
    gnb_test_scores.append(gnb.score(test_X, test_y))

print(logit_test_scores,gnb_test_scores)
sb.boxplot(data=[logit_test_scores, gnb_test_scores], notch=True)
plt.xticks([0, 1], ['LogisticRegression', 'GaussianNB'])
plt.ylabel('Test Accuracy')
plt.show()'''

'''import sdmetrics

# Load the demo data, which includes:
# - A dict containing the real tables as pandas.DataFrames.
# - A dict containing the synthetic clones of the real data.
# - A dict containing metadata about the tables.
real_data, synthetic_data, metadata = sdmetrics.load_demo()

# Obtain the list of multi table metrics, which is returned as a dict
# containing the metric names and the corresponding metric classes.
print(real_data)
'''
'''metrics = sdmetrics.multi_table.MultiTableMetric.get_subclasses()

# Run all the compatible metrics and get a report
print(sdmetrics.compute_metrics(metrics, real_data, synthetic_data, metadata=metadata))'''

from sdv.demo import load_tabular_demo

from sdv.tabular import GaussianCopula

real_data = load_tabular_demo('student_placements')

model = GaussianCopula()

model.fit(real_data)

synthetic_data = model.sample()

from sdv.evaluation import evaluate

print(evaluate(synthetic_data, real_data))