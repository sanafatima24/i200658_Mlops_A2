FROM apache/airflow:latest

USER root
RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

# Switch to the `airflow` user before installing Python packages
USER airflow

# Install DVC
RUN pip install dvc
RUN pip install dvc[gdrive]

# Switch back to root if you have further root-level commands
# USER root
