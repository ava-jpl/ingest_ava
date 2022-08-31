FROM hysds/pge-base:latest
MAINTAINER torresal "alexander.p.torres@jpl.nasa.gov"
LABEL description="Ingest products from metadata"

USER ops

COPY . /home/ops/verdi/ops/ingest_ava

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]

