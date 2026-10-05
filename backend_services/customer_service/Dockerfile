FROM ballerina/ballerina:2201.8.4
USER root
COPY . /home/ballerina/
RUN chown -R ballerina:ballerina /home/ballerina/
USER ballerina
WORKDIR /home/ballerina
RUN bal build
CMD ["bal", "run"]
