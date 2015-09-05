CREATE EXTENSION pg_shard;
CREATE TABLE u (
  id INTEGER,
  name TEXT
);

CREATE TABLE u_1 (
  id INTEGER,
  name TEXT
);

CREATE TABLE u_2 (
  id INTEGER,
  name TEXT
);

-- SELECT master_create_distributed_table('u', 'id');
-- SELECT master_create_worker_shards('u', 2, 1);

--INSERT INTO u VALUES (0, '0');
--INSERT INTO u VALUES (1, '1');
--INSERT INTO u VALUES (2, '2');

SELECT master_create_cluster('ucluster', 'INTEGER'::regtype, 2, 1);

BEGIN;
SELECT shard('ucluster', 0);
INSERT INTO u VALUES (0, '0');
COMMIT;

BEGIN;
SELECT shard('ucluster', 1);
INSERT INTO u VALUES (1, '1');
COMMIT;

BEGIN;
SELECT shard('ucluster', 2);
INSERT INTO u VALUES (2, '2');
COMMIT;

BEGIN;
SELECT shardall('ucluster');
--SELECT shard('ucluster', 2);
SELECT * FROM u;
SELECT * FROM u where id >= 1;
COMMIT;