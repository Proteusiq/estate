FROM python:3.10-slim

# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagit and dagster-daemon, and to load
# the DagsterInstance

RUN pip install \
    dagster \
    dagit \
    dagster-postgres \
    dagster-docker \
    requests \
    pandas \
    numpy \
    sqlalchemy \
    psycopg2-binary

# Set $DAGSTER_HOME and copy dagster instance there

ENV DAGSTER_HOME=/opt/dagster/dagster_home

RUN mkdir -p $DAGSTER_HOME

COPY dagster.yaml  $DAGSTER_HOME
COPY estates/repository.py workspace.yaml  /opt/dagster/app/

# Add repository code

WORKDIR /opt/dagster/app


# Run dagster gRPC server on port 4000

EXPOSE 4000

# Using CMD rather than ENTRYPOINT allows the command to be overridden in
# run launchers or executors to run other commands using this image
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "repository.py"]