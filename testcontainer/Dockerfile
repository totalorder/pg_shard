FROM ubuntu:14.04

MAINTAINER Anton Blomberg <totalorder@spotify.com>
RUN apt-get update

ADD tools /tools

ENV PGVERSION=9.3
ENV PG_PRELOAD=pg_shard
ENV HOST_USER=totalorder
#ENV enable_coverage=yes

RUN apt-get install -y make
RUN /tools/install_pg.sh
RUN /tools/nuke_pg.sh
RUN useradd $HOST_USER

RUN /tools/config_and_start_cluster.sh

CMD ["/bin/bash", "/tools/run_tests.sh"]