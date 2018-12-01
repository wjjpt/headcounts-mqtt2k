# headcount-mqtt2k

script for receiving data from mqtt, process and inject into kafka topic

# BUILDING

- Build docker image:
  * git clone https://github.com/wjjpt/headcounts-mqtt2k.git
  * cd src/
  * docker build -t wjjpt/headcountsm2k .

# EXECUTING

- Execute app using docker image:

`docker run --env KAFKA_BROKER=X.X.X.X --env KAFKA_PORT=9092 --env KAFKA_TOPIC='headcounts' -ti wjjpt/headcountsm2k`

