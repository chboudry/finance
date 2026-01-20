# Overview 

What we want to achieve here : 
- Transform data files to make them suitable for ingestion for the neo4j-admin import command
- Execute a full
- Execute an incremental

## Things you might need 

Set up path where your binaries are located (in my case the Home path refers to a classical Neo4j Desktop 2.0 path)

```
export NEO4J_HOME="/Users/chboudry/Library/Application Support/neo4j-desktop/Application/Data/dbmss/dbms-0c6b0d93-0a1f-4abf-a62b-ca587e4dc1c3"
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
```

## Prepare files

We need to transform the data for them to match the nodes and relationships formats it's going to have later on in the database.

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

Move the 3 DATE_Transactions+to+from.csv you want to use in a subfolder called "incremental"

## Run a Full 

1. Stop Neo4j (from your Neo4j installation folder) or the database :
```
$NEO4J_HOME/bin/neo4J stop
```

or 

```
STOP DATABASE finance
```

2. Run the import (database name is finance here):
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

- overwrite-destination will drop previous database
- id-type is going to be string for all files but it could get overriden within each headers
- schema is used to create constraints and index, it is not mandatory to have it for full but it will be mandatory to have them for the incremental to work
- it a a best practice to have headers in distinct files, it avoids opening potentially very large file just to get headers

3. Start Neo4j or the database:
```
$NEO4J_HOME/bin/neo4J start
```

or 

```
START DATABASE finance
```

## Run an incremental

Stopping the database during the incremental is unavoidable, but downtime can be reduced by a 3 staged process that is not showcased here.

1. Make sure you have set up constraints and indexes (we did taht using the schema file during the full)

2. Stop the database using Cypher query on system

```
STOP DATABASE finance WAIT
```

3. Run the incremental
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

4. Start Database again

```
START DATABASE finance
```