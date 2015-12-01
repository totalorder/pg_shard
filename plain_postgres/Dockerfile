FROM ubuntu:14.04

MAINTAINER Anton Blomberg <totalorder@spotify.com>
RUN apt-get update

ENV PGVERSION=9.3
RUN apt-get install -y sudo make postgresql-$PGVERSION postgresql-server-dev-$PGVERSION

RUN echo "local    all             all                            peer" > /etc/postgresql/$PGVERSION/main/pg_hba.conf
RUN echo "host    all             all             0.0.0.0/0               trust" >> /etc/postgresql/$PGVERSION/main/pg_hba.conf
RUN echo "host    all             all             ::1/128               trust" >> /etc/postgresql/$PGVERSION/main/pg_hba.conf
RUN echo "listen_addresses = '*'\n" >> /etc/postgresql/$PGVERSION/main/postgresql.conf
#RUN echo "log_statement = 'all'\n" >> /etc/postgresql/$PGVERSION/main/postgresql.conf

RUN echo "localhost 5432 shard0\nlocalhost 5432 shard1\nlocalhost 5432 shard2\nlocalhost 5432 shard3\nlocalhost 5432 shard4\nlocalhost 5432 shard5\nlocalhost 5432 shard6\nlocalhost 5432 shard7" > /var/lib/postgresql/$PGVERSION/main/pg_worker_list.conf

RUN (/etc/init.d/postgresql start && \
     sudo -u postgres psql -d postgres -c "CREATE DATABASE shard;" && \
     sudo -u postgres psql -d postgres -c "CREATE DATABASE shard0;" && \
sudo -u postgres psql -d postgres -c "CREATE DATABASE shard7;" && \
sudo -u postgres psql -d postgres -c "CREATE DATABASE shard1;" && \
sudo -u postgres psql -d postgres -c "CREATE DATABASE shard2;" && \
sudo -u postgres psql -d postgres -c "CREATE DATABASE shard3;" && \
sudo -u postgres psql -d postgres -c "CREATE DATABASE shard4;" && \
sudo -u postgres psql -d postgres -c "CREATE DATABASE shard5;" && \
     sudo -u postgres psql -d postgres -c "CREATE DATABASE shard6;")

RUN echo "shared_preload_libraries = 'pg_shard'" >> /etc/postgresql/$PGVERSION/main/postgresql.conf
ADD startup.sql /startup.sql
ADD startup /startup

CMD ["/bin/bash", "/startup"]
