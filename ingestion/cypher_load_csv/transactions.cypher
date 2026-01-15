// NOTE: The following script syntax is valid for database version 5.0 and above.
:param {
  // Define the file path root and the individual file names required for loading.
  // https://neo4j.com/docs/operations-manual/current/configuration/file-locations/
  file_path_root: 'file:////Users/chboudry/Code/finance/dataset/', // Change this to the folder your script can access the files at.
  file_1: 'HI-Small_Trans.csv' // Change this to your transactions file name.
};
// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database.
//
CREATE CONSTRAINT `Transaction_transactionId_uniq` IF NOT EXISTS
FOR (n:Transaction)
REQUIRE (n.transaction_id) IS UNIQUE;
// (Optional, recommended) Index for faster time filtering
CREATE INDEX Transaction_timestamp_date_idx IF NOT EXISTS
FOR (n:Transaction)
ON (n.timestamp_date);
:param {
  idsToSkip: []
};
// NODE load
// ---------
//
// Load Transaction nodes in batches. Nodes will be created using a MERGE statement to ensure uniqueness on transactionId.
//
// NOTE: Any transactions with IDs in the 'idsToSkip' list parameter will not be loaded.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row, linenumber() AS txId
WHERE NOT txId IN $idsToSkip
CALL (row, txId) {
  MERGE (t:Transaction { transaction_id: txId })
  SET t.timestamp = row.`Timestamp`
  // New parsed datetime field (source format: "YYYY/MM/DD HH:MM")
  SET t.timestamp_date =
    datetime(
      replace(substring(row.`Timestamp`, 0, 10), '/', '-') + 'T' +
      substring(row.`Timestamp`, 11, 5) + ':00'
    )
  SET t.from_bank = toInteger(trim(row.`From Bank`))
  SET t.from_account = row.`FromAccount`
  SET t.to_bank = toInteger(trim(row.`To Bank`))
  SET t.to_aAccount = row.`ToAccount`
  SET t.amount_received = toFloat(trim(row.`Amount Received`))
  SET t.receiving_currency = row.`Receiving Currency`
  SET t.amount_paid = toFloat(trim(row.`Amount Paid`))
  SET t.payment_currency = row.`Payment Currency`
  SET t.payment_format = row.`Payment Format`
  SET t.is_laundering = (row.`Is Laundering` = "1")
} IN TRANSACTIONS OF 10000 ROWS;
// RELATIONSHIP load
// -----------------
//
// Create FROM / TO relationships from Transaction to existing Account nodes.
// Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row, linenumber() AS txId
CALL (row, txId) {
  MATCH (t:Transaction { transaction_id: txId })
  MATCH (fromAcc:Account { account_number: row.`FromAccount` })
  MERGE (t)-[:FROM]->(fromAcc)
} IN TRANSACTIONS OF 10000 ROWS;
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row, linenumber() AS txId
CALL (row, txId) {
  MATCH (t: Transaction { transaction_id: txId})
  MATCH (toAcc: Account { account_number: row.`ToAccount` })
  MERGE (t)-[:TO]->(toAcc)
} IN TRANSACTIONS OF 10000 ROWS;