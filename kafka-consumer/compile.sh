rm -rf ./deploy
mkdir ./deploy
cp ./etc/* ./deploy
mvn clean compile assembly:single
cp ./target/kafka-consumer-1.0-SNAPSHOT-jar-with-dependencies.jar ./deploy