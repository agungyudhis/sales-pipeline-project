# First-time build can take upto 10 mins.

FROM apache/airflow:slim-2.10.2

ENV AIRFLOW_HOME=/opt/airflow

# Create the user

WORKDIR $AIRFLOW_HOME

USER root
RUN apt update -q
RUN apt install software-properties-common -qqq
RUN apt install python3-launchpadlib -qqq
RUN apt install build-essential -qqq
RUN apt install pkg-config -qqq

COPY requirements.txt .

RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --no-cache-dir -r requirements.txt

RUN mkdir /home/data

# Create the AIRFLOW_HOME directory and change its ownership to the airflow user
RUN chown -R airflow: ${AIRFLOW_HOME}

# Switch back to the airflow user
USER airflow

USER ${AIRFLOW_UID}