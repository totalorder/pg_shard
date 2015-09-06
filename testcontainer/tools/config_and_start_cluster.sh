#!/bin/bash

# Inspired by https://gist.github.com/petere/6023944

set -eux

# Create fully-trusting cluster on custom port, owned by us
pg_createcluster $PGVERSION test -p 55435 -u $HOST_USER -- -A trust

# Preload library if asked to do so
if [ ${PG_PRELOAD+1} ]
then
  echo "shared_preload_libraries = '$PG_PRELOAD'" >> \
    /etc/postgresql/$PGVERSION/test/postgresql.conf
fi

