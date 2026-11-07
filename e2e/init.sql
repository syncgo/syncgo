-- Executed once when the e2e postgres volume is initialized.
-- The table used by the pipeline e2e tests and the syncgo prepare command.
CREATE TABLE IF NOT EXISTS test (id int primary key, name text);

-- The filtered table of the shared publication (see e2e/config.yaml):
-- syncgo prepare adds it to the publication with a row filter, so it must
-- exist before prepare runs.
CREATE TABLE IF NOT EXISTS syncgo_filter_pipeline (id int primary key, name text);
