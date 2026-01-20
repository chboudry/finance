CREATE CONSTRAINT `Bank_ID_Bank_uniq` IF NOT EXISTS
FOR (n: Bank)
REQUIRE (n.bank_id) IS UNIQUE;

CREATE CONSTRAINT `Account_Number_Account_uniq` IF NOT EXISTS
FOR (n: Account)
REQUIRE (n.account_number) IS UNIQUE;

CREATE CONSTRAINT `Entity_ID_Entity_uniq` IF NOT EXISTS
FOR (n: Entity)
REQUIRE (n.entity_id) IS UNIQUE;

CREATE CONSTRAINT `Transaction_transactionId_uniq` IF NOT EXISTS
FOR (n:Transaction)
REQUIRE (n.transaction_id) IS UNIQUE;

// (Optional, recommended) Index for faster time filtering
CREATE INDEX Transaction_timestamp_date_idx IF NOT EXISTS
FOR (n:Transaction)
ON (n.timestamp_date);