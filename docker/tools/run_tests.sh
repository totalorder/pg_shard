#!/bin/bash

# Inspired by https://gist.github.com/petere/6023944

set -eux

cd /pg_shard

status=0

# Build and install extension
make all PG_CONFIG=/usr/lib/postgresql/$PGVERSION/bin/pg_config
make install PG_CONFIG=/usr/lib/postgresql/$PGVERSION/bin/pg_config

# Start cluster
sudo pg_ctlcluster $PGVERSION test start

# Run tests. DBs owned by non-standard owner put socket in /tmp
PGHOST=/tmp PGPORT=55435 make installcheck PGUSER=$HOST_USER PG_CONFIG=/usr/lib/postgresql/$PGVERSION/bin/pg_config || status=$?

# Print diff if it exists
if test -f regression.diffs; then cat regression.diffs; fi

exit $status
