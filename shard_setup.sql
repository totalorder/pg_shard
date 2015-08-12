CREATE EXTENSION pg_shard;
CREATE TABLE u (
  id INTEGER,
  name TEXT
);

SELECT master_create_distributed_table('u', 'id');
SELECT master_create_worker_shards('u', 2, 1);

INSERT INTO u VALUES (0, '0');
INSERT INTO u VALUES (1, '1');
INSERT INTO u VALUES (2, '2');