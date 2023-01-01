FROM ubuntu:focal
ENV HOME /dbrest
RUN mkdir /dbrest
WORKDIR /dbrest
COPY dbrest /dbrest/
RUN chmod -R 777 /dbrest && chmod +x /dbrest/dbrest
ENTRYPOINT ["/dbrest/dbrest"]