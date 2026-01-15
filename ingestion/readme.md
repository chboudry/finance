# How to use 

- download dataset from [kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) and dezip into dataset folder

- run script.py (rename the default headers because "account" exists twice and this can create issue)
```
cd dataset
python3 ../script.py
```

- Pick the sub-folder associated with the method you want to use to do the import

## option 1 : neo4j-admin data import

- we need to transform the data for them to match the nodes and relationships formats it's goind to have later on in the database.

```
python3 ingestion/data_importer/transform_f1.py \
	--input dataset/LI-Small_accounts.csv \
	--out-dir datatransformed
```

- Stop Neo4j (from your Neo4j installation folder):
```
/Users/chboudry/Library/Application\ Support/neo4j-desktop/Application/Data/dbmss/dbms-0c6b0d93-0a1f-4abf-a62b-ca587e4dc1c3/bin/neo4J stop
```

- Run the import (adjust the database name if needed):
```
/Users/chboudry/Library/Application\ Support/neo4j-desktop/Application/Data/dbmss/dbms-0c6b0d93-0a1f-4abf-a62b-ca587e4dc1c3/bin/neo4j-admin database import full finance \
	--overwrite-destination=true \
	--id-type=string \
	--nodes=Bank=datatransformed/banks.csv \
	--nodes=Entity=datatransformed/entities.csv \
	--nodes=Account=datatransformed/accounts.csv \
	--relationships=OWNS=datatransformed/entity_owns_account.csv \
	--relationships=PART_OF=datatransformed/account_part_of_bank.csv
```

- Start Neo4j:
```
/Users/chboudry/Library/Application\ Support/neo4j-desktop/Application/Data/dbmss/dbms-0c6b0d93-0a1f-4abf-a62b-ca587e4dc1c3/bin/neo4J start
```



## option 2 : cypher LOAD CSV
