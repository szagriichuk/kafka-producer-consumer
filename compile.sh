cd ./kafka-common
mvn clean install

cd ../kafka-producer
sh ./compile.sh

cd ../kafka-consumer
sh ./compile.sh