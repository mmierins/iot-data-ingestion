
# iot-data-ingestion
## Configuration of services

**Create kafka topic**

    kafka-topics.sh --create --zookeeper <hostname> --replication-factor <replication-factor> --partitions <partitions> --topic temperatures

**Create hbase table via *hbase shell***

    create 'temprs', {NAME => 't', VERSIONS => 1}, {NAME => 'r', VERSIONS => 1}

**Create impala table via *hive shell***

    CREATE EXTERNAL TABLE temperatures (
	    id string, 
	    deviceId string,
	    temperature int,
	    longitude String, 
	    latitude String,
	    time string,
	    json string) 
	STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
	WITH SERDEPROPERTIES ( 
		"hbase.columns.mapping" = ":key,t:de,t:te#b,t:lo,t:la,t:ts,r:json" 
	) TBLPROPERTIES("hbase.table.name" = "temprs");


**Build project** 
mvn package

**Run simulator app**

    java -jar iot-device-simulator-1.0-SNAPSHOT.jar

(default config can be overriden by placing config/application.properties file into dir where jar resides)

**Run streaming app**

    spark-submit2 --class app.StreamingApp --master <spark_master_host> --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --properties-file streaming.properties --files hbase-site.xml --driver-class-path . kafka-to-spark-streamer-1.0-SNAPSHOT.jar

It is expected that *hbase-site.xml* and *streaming.properties* files are located in the same dir as built jar.
Example of contents of *streaming.properties* file:

    spark.kafka.bootstrap.servers=<kafka_hostname>
    spark.kafka.topic.name=temperatures
    spark.hbase.table.name=temprs
    
    spark.executor.extraClassPath=./

## SQL queries

**Max measured temp per device**

    select deviceId, max(temperature)
    from <table>
    group by deviceId

**Collected number of data points per device**

    select deviceId, count(*)
    from <table>
    group by deviceId

**Max temp on a specific day per device**

    select deviceId, max(temperature)
    from temperatures
    where trunc(time, 'DD') = '2018-11-20'
    group by deviceId
