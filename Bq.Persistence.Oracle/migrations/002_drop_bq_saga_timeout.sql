-- Delayed sends and saga timeouts are no longer kept in a table: they are outbox rows that the broker holds until they are due.
-- Run this on a database created by 001 before this change, once its endpoints no longer have rows in bq_saga_timeout.
-- Check first: SELECT COUNT(*) FROM bq_saga_timeout;
DROP TABLE bq_saga_timeout;
