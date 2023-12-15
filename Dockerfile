FROM apache/airflow:2.4.1

USER root
# RUN apt-get update && apt-get -y install sudo
# RUN apt -y full-upgrade
# RUN apt-get install -y libpq-dev python-dev
# RUN apt install -y libpython3-dev
# RUN apt-get -y install gcc
RUN apt-get update \
    && apt install gcc python3-dev libpq-dev openjdk-11-jdk -y

# Add none root user
# RUN  useradd airflow && echo "airflow:airflow" | chpasswd && adduser airflow sudo
USER airflow
RUN pip3 install --upgrade pip

COPY requirements.txt .
RUN pip3 install -r requirements.txt