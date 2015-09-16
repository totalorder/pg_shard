# Sharding Postgres for horizontal scaling with full support for transactions and joins

## Getting started
Clone the repo:
`git clone https://github.com/totalorder/pg_shard.git`

Checkout the shard-transaction branch:
`git checkout shard-transaction`

Spin up a docker container which compiles the
code and sets up a cluster for you:
```bash
# Install docker if you don't already have it

cd compilecontainer/

./build
./run
```

This will compile the code and you will be dropped
into a `psql` shell on a postgres server with shards already set up.
The front-end lives in the database `shard`
The backends live in the databases `shard0` and `shard1`

Take a look in `manual_test.sql` to see what
commands are available.