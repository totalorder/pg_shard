SCRIPT_DIR="$(readlink -f `dirname $0`)"
REPO_DIR="$(readlink -f $SCRIPT_DIR/..)"

if [ -z "$(docker images | grep pg_shard-test)" ]; then
    docker build -t pg_shard-test $SCRIPT_DIR
fi

docker rm -f pg_shard-test ; docker run -v $REPO_DIR:/pg_shard -i -t --name pg_shard-test pg_shard-test

