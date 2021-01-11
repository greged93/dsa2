#!/bin/sh

alias python=python3.8

touch few_results.txt
echo "" > few_results.txt
touch feat_results.txt
echo "" > feat_results.txt

python ./few-experiments/few_testing.py ../data/input/jane/train/train01 ../data/input/jane/train/train02 ./genetic_algo_params.txt &> few_results.txt
# conda activate feat-env
python ./feat-experiments/feat_testing.py ../data/input/jane/train/train01 ../data/input/jane/train/train02 ./genetic_algo_params.txt &> feat_results.txt