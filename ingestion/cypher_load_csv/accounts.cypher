// NOTE: The following script syntax is valid for database version 5.0 and above.
:param {
  // Define the file path root and the individual file names required for loading.
  // https://neo4j.com/docs/operations-manual/current/configuration/file-locations/
  file_path_root: 'file:////Users/chboudry/Code/finance/dataset/', // Change this to the folder your script can access the files at.
  file_0: 'HI-Small_accounts.csv'
};
// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
CREATE CONSTRAINT `Bank_ID_Bank_uniq` IF NOT EXISTS
FOR (n: Bank)
REQUIRE (n.bank_id) IS UNIQUE;
CREATE CONSTRAINT `Account_Number_Account_uniq` IF NOT EXISTS
FOR (n: Account)
REQUIRE (n.account_number) IS UNIQUE;
CREATE CONSTRAINT `Entity_ID_Entity_uniq` IF NOT EXISTS
FOR (n: Entity)
REQUIRE (n.entity_id) IS UNIQUE;
:param {
  idsToSkip: []
};
// NODE load
// ---------
//
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
//
// NOTE: Any nodes with IDs in the 'idsToSkip' list parameter will not be loaded.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Bank ID` IN $idsToSkip AND NOT toInteger(trim(row.`Bank ID`)) IS NULL
CALL (row) {
  MERGE (n:Bank { bank_id: toInteger(trim(row.`Bank ID`)) })
  SET n.bank_name = row.`Bank Name`
} IN TRANSACTIONS OF 10000 ROWS;
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Account Number` IN $idsToSkip AND NOT row.`Account Number` IS NULL
CALL (row) {
  MERGE (n:Account { account_number: row.`Account Number` })
} IN TRANSACTIONS OF 10000 ROWS;
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Entity ID` IN $idsToSkip AND NOT row.`Entity ID` IS NULL
CALL (row) {
  MERGE (n:Entity { entity_id: row.`Entity ID` })
  SET n.entity_name = row.`Entity Name`
} IN TRANSACTIONS OF 10000 ROWS;
// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source:Entity { entity_id: row.`Entity ID` })
  MATCH (target:Account { account_number: row.`Account Number` })
  MERGE (source)-[r:OWNS]->(target)
} IN TRANSACTIONS OF 10000 ROWS;
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source:Account { account_number: row.`Account Number` })
  MATCH (target:Bank { bank_id: toInteger(trim(row.`Bank ID`)) })
  MERGE (source)-[r:PART_OF]->(target)
} IN TRANSACTIONS OF 10000 ROWS;