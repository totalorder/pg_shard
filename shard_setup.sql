BEGIN; COMMIT;
CREATE EXTENSION pg_shard;

SELECT master_create_cluster('ucluster', 'INTEGER'::regtype, 2, 1);

BEGIN;
SELECT shardall('ucluster');
CREATE TABLE u (
  id INTEGER,
  name TEXT
);
COMMIT;

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
SELECT * FROM u;
SELECT * FROM u WHERE id >= 1;
COMMIT;

BEGIN;
SELECT shard('ucluster', 0);
UPDATE u SET name = '00' WHERE id = 0;
SELECT * FROM u;
ROLLBACK;

BEGIN;
SELECT shard('ucluster', 0);
UPDATE u SET name = '11' WHERE id = 1;
SELECT * FROM u;
COMMIT;

BEGIN;
SELECT shardall('ucluster');
SELECT * FROM u;
COMMIT;

BEGIN;
SELECT shardall('ucluster');
SELECT COUNT(id) FROM u;
COMMIT;

BEGIN;
SELECT shardall('ucluster');
SELECT * FROM u;
COMMIT;
