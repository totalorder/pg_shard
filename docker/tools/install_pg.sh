#!/bin/bash

# Inspired by https://gist.github.com/petere/6023944

set -eux

# always install postgresql-common
packages="postgresql-common"

packages="$packages postgresql-$PGVERSION postgresql-server-dev-$PGVERSION"

apt-get -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" install -y $packages
