FROM --platform=linux/amd64 debezium/connect:2.7.3.Final

USER root
COPY plugins/kafka-connect-s3.zip /opt/kafka-connect-s3.zip
RUN unzip /opt/kafka-connect-s3.zip -d /kafka/connect/ && \
    rm /opt/kafka-connect-s3.zip
USER 1001
