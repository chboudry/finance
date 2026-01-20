# neo4j-admin data import full for csv

1. Set up path where your binary are located

```
export NEO4J_HOME="/Users/chboudry/Library/Application Support/neo4j-desktop/Application/Data/dbmss/dbms-0c6b0d93-0a1f-4abf-a62b-ca587e4dc1c3"
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
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

3. Stop Neo4j (from your Neo4j installation folder) or the database :
```
$NEO4J_HOME/bin/neo4J stop
```

or 

```
STOP DATABASE finance
```

4. Run the import (database name is finance here):
```
$NEO4J_HOME/bin/neo4j-admin database import full finance \
	--overwrite-destination=true \
	--id-type=string \
	--schema=ingestion/data_import/schema/schema.cypher \
	--nodes=Bank=ingestion/data_import/headers/banks_header.csv,datatransformed/banks.csv \
	--nodes=Entity=ingestion/data_import/headers/entities_header.csv,datatransformed/entities.csv \
	--nodes=Account=ingestion/data_import/headers/accounts_header.csv,datatransformed/accounts.csv \
	--nodes=Transaction=ingestion/data_import/headers/transactions_header.csv,"datatransformed/[0-9]{4}_[0-9]{2}_[0-9]{2}_transactions.csv" \
	--relationships=OWNS=ingestion/data_import/headers/entity_owns_account_header.csv,datatransformed/entity_owns_account.csv \
	--relationships=PART_OF=ingestion/data_import/headers/account_part_of_bank_header.csv,datatransformed/account_part_of_bank.csv \
	--relationships=FROM=ingestion/data_import/headers/transactions_from_header.csv,"datatransformed/[0-9]{4}_[0-9]{2}_[0-9]{2}_transactions_from.csv" \
	--relationships=TO=ingestion/data_import/headers/transactions_to_header.csv,"datatransformed/[0-9]{4}_[0-9]{2}_[0-9]{2}_transactions_to.csv" 
```


5. Start Neo4j or the database:
```
$NEO4J_HOME/bin/neo4J start
```

or 

```
START DATABASE finance
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

2. Make sure you have set up constraints and indexes (done by schema file on full)

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
	--nodes=Account=ingestion/data_import/headers/accounts_header.csv,datatransformed/incremental/accounts.csv \
	--nodes=Transaction=ingestion/data_import/headers/transactions_header.csv,"datatransformed/incremental/[0-9]{4}_[0-9]{2}_[0-9]{2}_transactions.csv" \
	--relationships=FROM=ingestion/data_import/headers/transactions_from_header.csv,"datatransformed/incremental/[0-9]{4}_[0-9]{2}_[0-9]{2}_transactions_from.csv" \
	--relationships=TO=ingestion/data_import/headers/transactions_to_header.csv,"datatransformed/incremental/[0-9]{4}_[0-9]{2}_[0-9]{2}_transactions_to.csv" 
```

5. Start Database again

```
START DATABASE finance
```