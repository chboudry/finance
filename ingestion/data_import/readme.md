# neo4j-admin data import full for csv

1. Set up path where your binary are located

```
export NEO4J_BIN="/Users/chboudry/Library/Application Support/neo4j-desktop/Application/Data/dbmss/dbms-0c6b0d93-0a1f-4abf-a62b-ca587e4dc1c3/bin"
```

2. We need to transform the data for them to match the nodes and relationships formats it's going to have later on in the database.

```
python3 ingestion/data_import/transform_accounts.py \
	--input dataset/LI-Small_accounts.csv \
	--out-dir datatransformed
```

```
python3 ingestion/data_import/transform_transactions.py \
	--input dataset/LI-Small_Trans.csv \
	--out-dir datatransformed \
    --split-by-date false
```
Use split-by-date true if you want to test data import incremental later on.

3. Stop Neo4j (from your Neo4j installation folder):

```
$NEO4J_BIN/neo4J stop
```

4. Run the import (adjust the database name if needed):
```
$NEO4J_BIN/neo4j-admin database import full finance \
	--overwrite-destination=true \
	--id-type=string \
	--nodes=Bank=datatransformed/banks.csv \
	--nodes=Entity=datatransformed/entities.csv \
	--nodes=Account=datatransformed/accounts.csv \
	--nodes=Transaction=datatransformed/*_transactions.csv \
	--relationships=OWNS=datatransformed/entity_owns_account.csv \
	--relationships=PART_OF=datatransformed/account_part_of_bank.csv \
	--relationships=FROM=datatransformed/*_transaction_from.csv \
	--relationships=TO=datatransformed/*_transaction_to.csv
```

5. Start Neo4j:
```
$NEO4J_BIN/neo4J start
```

# neo4j-admin data import incremental

Incremental import expects **delta files** (only new nodes / relationships). Re-importing the full dataset can create duplicates (or be rejected by constraints).

1. Follow the tutorial with the full with thoses changes: 
- For fransform_transactions.py, use split-by-date to true
- Pick a date of your choices and rename the 3 files files (DATE_transactions, DATE_transactions_to and DATE_transactions_from) to incremental, incrementals_to and incrementals_from for instance. 

2. 

# neo4j-admin data import full for parquet