rm -rf ./deploy

cd ./kafka-common
mvn clean install

cd ../kafka-producer
sh ./compile.sh

cd ../kafka-consumer
sh ./compile.sh

cd ../

mkdir deploy
mkdir deploy/producer
mkdir deploy/consumer

cp ./kafka-producer/deploy/* ./deploy/producer
cp ./kafka-consumer/deploy/* ./deploy/consumer