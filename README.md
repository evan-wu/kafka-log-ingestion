Simple utility to import message from kafka to database.

## Configuration
config.properties:

```
kafka.group.id = LOG-TO-MYSQL
kafka.bootstrap.servers = localhost:9092
kafka.auto.offset.reset = earliest

log.topic.mappings = myLogTopic=test.log_table
log.ingestion.class=com.tazhi.log.ingestion.jdbc.JdbcIngestion
#
jdbc.driverClass = com.mysql.jdbc.Driver
jdbc.url = jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false
jdbc.user = test
jdbc.password = test1234
```
Any properties begins with 'kafka.' will be applied to the kafka consumer.

## Build and Run
```
maven clean install
java -jar kafka-log-ingestion-1.0-SNAPSHOT.jar (in target folder)
```
