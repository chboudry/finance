# How to use 

- download dataset from [kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) and unzip into dataset folder

- run script.py to rename the default headers and make them unique (by default "account" exists twice)
```
cd dataset
python3 ingestion/script.py
```

- Pick the sub-folder associated with the method you want to use to do the import
