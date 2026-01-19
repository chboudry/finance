# neo4j-admin data import full for csv

1. Set up path where your binary are located

```
export NEO4J_HOME="/Users/chboudry/Library/Application Support/neo4j-desktop/Application/Data/dbmss/dbms-0c6b0d93-0a1f-4abf-a62b-ca587e4dc1c3"
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
    --split-by-date true
```
Use split-by-date true if you want to test data import incremental later on.

3. Stop Neo4j (from your Neo4j installation folder):

```
$NEO4J_HOME/bin/neo4J stop
```

4. Run the import (adjust the database name if needed):
```
$NEO4J_HOME/bin/neo4j-admin database import full finance \
	--overwrite-destination=true \
	--auto-skip-subsequent-headers=true \
	--id-type=string \
	--nodes=Bank=datatransformed/banks.csv \
	--nodes=Entity=datatransformed/entities.csv \
	--nodes=Account=datatransformed/accounts.csv \
	--nodes=Transaction="datatransformed/[0-9]{4}_[0-9]{2}_[0-9]{2}_transactions.csv" \
	--relationships=OWNS=datatransformed/entity_owns_account.csv \
	--relationships=PART_OF=datatransformed/account_part_of_bank.csv \
	--relationships=FROM="datatransformed/[0-9]{4}_[0-9]{2}_[0-9]{2}_transactions_from.csv" \
	--relationships=TO="datatransformed/[0-9]{4}_[0-9]{2}_[0-9]{2}_transactions_to.csv" 
```


5. Start Neo4j:
```
$NEO4J_HOME/bin/neo4J start
```

# neo4j-admin data import incremental

Incremental import expects **delta files** (only new nodes / relationships). Re-importing the full dataset can create duplicates (or be rejected by constraints).

Stopping the database during the incremental is unavoidable, but downtime can be reduced by a 3 staged process that is not showcased here.

1. Follow the tutorial with the full with thoses changes: 
- For fransform_transactions.py, use split-by-date to true
- stop Neo4j 
- run the full
- start Neo4j
- move dates you want to ingest using incrementals in a sub folder (or another folder)
- also copy accounts (or only the accounts that are used by the date_transactions) into the same subfolder

2. Make sure you have set up constraints and indexes 

3. Stop the database using Cypher query on system

```
STOP DATABASE finance WAIT
```

4. Run the incremental
```
$NEO4J_HOME/bin/neo4j-admin database import incremental finance \
	--force \
	--stage=all \
	--id-type=string \
	--skip-duplicate-nodes=true \
	--nodes=Account=datatransformed/incremental/accounts.csv \
	--nodes=Transaction="datatransformed/incremental/[0-9]{4}_[0-9]{2}_[0-9]{2}_transactions.csv" \
	--relationships=FROM="datatransformed/incremental/[0-9]{4}_[0-9]{2}_[0-9]{2}_transactions_from.csv" \
	--relationships=TO="datatransformed/incremental/[0-9]{4}_[0-9]{2}_[0-9]{2}_transactions_to.csv" 
```

5. Start Database again

```
START DATABASE finance
```