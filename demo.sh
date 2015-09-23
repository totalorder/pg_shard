#!/bin/bash
clear

demo () {
  echo ""
  echo "$1"
  read t
  psql -d shard -f <(echo "$1")
  read t
  clear
}

#demo "-- Create the pg_shard extension
#CREATE EXTENSION pg_shard;"

demo "-- Create a cluster named 'userid' with the sharding key
-- datatype INTEGER with 2 shards
SELECT create_cluster('userid', 'INTEGER'::regtype, 2);"


demo "-- Create a table 'usr'
-- The table get created in all shards by using shardall()
BEGIN;
  SELECT shardall('userid');

  CREATE TABLE usr (
    id INTEGER PRIMARY KEY,
    name TEXT
  );
COMMIT;"

demo "-- Create a user
-- Since shard() is used the user will be created
-- on the correct shard based on the userid
BEGIN;
  SELECT shard('userid', 0);
  INSERT INTO usr (id, name) VALUES (0, 'usr0');
COMMIT;"

demo "-- Create two more users
BEGIN;
  SELECT shard('userid', 1);
  INSERT INTO usr (id, name) VALUES (1, 'usr1');
COMMIT;

BEGIN;
  SELECT shard('userid', 2);
  INSERT INTO usr (id, name) VALUES (2, 'usr2');
COMMIT;"

demo "-- Verify that the users got persisted by
-- selecting across all shards with shardall()
BEGIN;
  SELECT shardall('userid');
  SELECT * FROM usr;
COMMIT;"

demo "-- Update a user, verify that the update can
-- be seen and then rollback the transaction
BEGIN;
  SELECT shard('userid', 0);
  UPDATE usr SET name = 'usr00' WHERE id = 0;
  SELECT * FROM usr WHERE id = 0;
ROLLBACK;"

demo "-- Update a user and commit the changes
BEGIN;
  SELECT shard('userid', 0);
  UPDATE usr SET name = 'usr11' WHERE id = 1;
  SELECT * FROM usr WHERE id = 1;
COMMIT;"

demo "-- Verify that one update was rolled back and
-- one update was persisted across all shards
BEGIN;
  SELECT shardall('userid');
  SELECT * FROM usr;
COMMIT;"

demo "-- Test that COUNT() works across all shards
BEGIN;
  SELECT shardall('userid');
  SELECT COUNT(id) FROM usr;
COMMIT;"

demo "-- Create a table for todo-items
-- Todo items have a foreign key to usr
BEGIN;
  SELECT shardall('userid');
  CREATE TABLE todo (
    id INTEGER PRIMARY KEY,
    usr_id INTEGER REFERENCES usr,
    text TEXT
  );
COMMIT;"

demo "-- Create todo-items for the user 0 and 1
-- Sequences are not yet supported so we have
-- to specify the IDs of the todos
BEGIN;
  SELECT shard('userid', 0);
  INSERT INTO todo (id, usr_id, text) VALUES (0, 0, 'todo 0 for user 0');
COMMIT;

BEGIN;
  SELECT shard('userid', 0);
  INSERT INTO todo (id, usr_id, text) VALUES (1, 0, 'todo 1 for user 0');
COMMIT;

BEGIN;
  SELECT shard('userid', 1);
  INSERT INTO todo (id, usr_id, text) VALUES (2, 1, 'todo 0 for user 1');
COMMIT;"

demo "-- Verify that the todo-items for user 0 can
-- be selected with a JOIN
BEGIN;
  SELECT shard('userid', 0);
  SELECT * FROM usr JOIN todo ON (usr.id = todo.usr_id) WHERE usr.id = 0;
COMMIT;"

demo "-- Verify that the todo-items for user 1 can
-- be selected with a JOIN
BEGIN;
  SELECT shard('userid', 1);
  SELECT * FROM usr JOIN todo ON (usr.id = todo.usr_id) WHERE usr.id = 1;
COMMIT;"

demo "-- Verify that no todos are found for user 2
BEGIN;
  SELECT shard('userid', 2);
  SELECT * FROM usr JOIN todo ON (usr.id = todo.usr_id) WHERE usr.id = 2;
COMMIT;"


echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo ""
echo "    Thanks!"
echo ""
echo "    Anton Blomberg"
echo "    https://github.com/totalorder/pg_shard"
echo "    totalorder@"
