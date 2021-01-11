#!/bin/sh
alias python=python3.8
touch all_sklearn_results.txt
echo "" > all_sklearn_results.txt
echo "=========="
python few_sklearn.py boston >> all_sklearn_results.txt
echo "=========="
python few_sklearn.py iris >> all_sklearn_results.txt
echo "=========="
python few_sklearn.py diabetes >> all_sklearn_results.txt
echo "=========="
python few_sklearn.py digits >> all_sklearn_results.txt
echo "=========="
python few_sklearn.py linnerud >> all_sklearn_results.txt
echo "=========="
python few_sklearn.py wine >> all_sklearn_results.txt
echo "=========="
python few_sklearn.py cancer >> all_sklearn_results.txt
echo "Done!"