MAINTAINER Anton Blomberg <totalorder@spotify.com>
RUN apt-get update

ENV HOST_USER=totalorder
RUN useradd $HOST_USER

RUN apt-get install -y postgresql sudo

RUN echo "host    all             all             0.0.0.0/0               trust" >> /etc/postgresql/9.3/main/pg_hba.conf
RUN echo "listen_addresses = '*'" >> /etc/postgresql/9.3/main/postgresql.conf

RUN (/etc/init.d/postgresql start && \
     sudo -u postgres psql -d postgres -c "CREATE DATABASE shard" && \
     sudo -u postgres psql -d postgres -c "CREATE USER totalorder CREATEUSER" && \
     sudo -u totalorder psql -d shard --c "CREATE TABLE u (id INTEGER, name TEXT);")

RUN echo "/etc/init.d/postgresql start && sudo -u $HOST_USER bash -c 'psql -d shard'" > /start && chmod a+x /start
RUN cat /start
CMD ["bash", "/start"]