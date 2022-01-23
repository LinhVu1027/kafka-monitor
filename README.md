```sh
$ docker-compose exec kafka bash

$ kafka-console-producer \
 --bootstrap-server kafka:9092 \
 --topic topic1

$ kafka-console-consumer \
 --bootstrap-server kafka:9092 \
 --topic topic1 \
 --from-beginning
 
 $ kafka-console-consumer \
 --bootstrap-server kafka:9092 \
 --topic products-dlq \
 --value-deserializer org.apache.kafka.common. serialization.IntegerDeserializer \
 --from-beginning
 
$ kafka-console-consumer \
 --bootstrap-server kafka:9092 \
 --topic submitted-products \
 --from-beginning

$ kafka-console-consumer \
 --bootstrap-server kafka:9092 \
 --topic invoices \
 --from-beginning
 
$ kafka-console-consumer \
 --bootstrap-server kafka:9092 \
 --topic invoice1s-dlq \
 --from-beginning
```