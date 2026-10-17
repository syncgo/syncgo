-- Executed once when the e2e postgres volume is initialized.
-- The table used by the pipeline e2e tests and the syncgo prepare command.
CREATE TABLE IF NOT EXISTS test (id int primary key, name text);
