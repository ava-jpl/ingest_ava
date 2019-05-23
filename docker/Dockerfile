FROM hysds/pge-base:latest
MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="Ingest products from metadata"

USER ops

COPY . /home/ops/verdi/ops/ingest_ava

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]

