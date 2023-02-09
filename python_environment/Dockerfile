FROM python:3.8-slim-buster

ARG project 
# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

COPY . .

RUN pip install -r requirements.txt  

COPY profiles.yml /root/.dbt/profiles.yml

ENV KSU_CONF_FILE ../../conf.ini
ENV GOOGLE_APPLICATION_CREDENTIALS=../dbt/google/google_credentials.json
ENV GOOGLE_CLOUD_PROJECT=$project

WORKDIR /volume/