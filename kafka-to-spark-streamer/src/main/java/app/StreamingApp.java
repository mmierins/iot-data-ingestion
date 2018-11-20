package app;

import domain.DataEnvelope;
import misc.MD5Hasher;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.xml.PrettyPrinter;
import writer.HBaseForeachWriter;

import javax.xml.bind.DatatypeConverter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;

public class StreamingApp {

    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Kafka2HbaseStreamingApp")
                .getOrCreate();

        Map<String, String> params = getParams(spark);

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", params.get(Params.KAFKA_BOOTSTRAP.fullname))
                .option("subscribe", params.get(Params.KAFKA_TOPIC_NAME.fullname))
                .load();

        Encoder<DataEnvelope> enc = Encoders.bean(DataEnvelope.class);

        List<Column> cols = new ArrayList<>();
        cols.add(col("json"));
        flattenSchema(enc.schema(), "data").forEach(cols::add);

        StreamingQuery query = df
                .selectExpr("CAST(value AS STRING) as json")
                .select(col("json"), from_json(col("json"), enc.schema()).as("data"))
                .select(cols.toArray(new Column[cols.size()]))
                .withColumn("time", from_unixtime(col("time"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
                .withColumnRenamed("deviceId", "de")
                .withColumnRenamed("temperature", "te")
                .withColumnRenamed("longitude", "lo")
                .withColumnRenamed("latitude", "la")
                .withColumnRenamed("time", "ts")
                .writeStream()
                .trigger(Trigger.ProcessingTime("1 seconds"))
                .foreach(new HBaseForeachWriter<Row>(params.get(Params.HBASE_TABLE_NAME.fullname)) {
                    MD5Hasher hasher = new MD5Hasher();

                    byte[] toBytes(Object x) {
                        if (x instanceof Integer) {
                            return Bytes.toBytes((int) x);
                        } else if (x instanceof Long) {
                            return Bytes.toBytes((long) x);
                        } else if (x instanceof String) {
                            return Bytes.toBytes((String) x);
                        } else {
                            throw new RuntimeException("No conversion to bytes defined for class " + x.getClass());
                        }
                    }

                    byte[] composeKey(Row record) {
                        String key = String.format("%s_%s", record.<String>getAs("de"), record.<String>getAs("ts"));
                        return hasher.hash(key);
                    }

                    @Override
                    public Put toPut(Row record) {
                        byte[] key = composeKey(record);

                        String columnFamilyName1 = "t";
                        String columnFamilyName2 = "r";
                        List<String> rColFamilyColumns = Arrays.asList("json");

                        Put p = new Put(DatatypeConverter.printHexBinary(key).toUpperCase().getBytes());
                        List<String> fieldNames = Stream.of(record.schema().fields()).map(f -> f.name()).collect(Collectors.toList());
                        scala.collection.immutable.Map<String, Object> scalaMap = record.getValuesMap(scala.collection.JavaConversions.asScalaBuffer(fieldNames).toSeq());
                        Map<String, Object> m = scala.collection.JavaConversions.mapAsJavaMap(scalaMap);

                        m.forEach((columnName, columnValue) -> {
                            String colFamilyName = rColFamilyColumns.contains(columnName) ? columnFamilyName2 : columnFamilyName1;
                            p.addColumn(Bytes.toBytes(colFamilyName), Bytes.toBytes(columnName), toBytes(columnValue));
                        });

                        return p;
                    }
                })
                .start();

        query.awaitTermination();
        spark.close();
    }

    enum Params {
        KAFKA_BOOTSTRAP("spark.kafka.bootstrap.servers"),
        KAFKA_TOPIC_NAME("spark.kafka.topic.name"),
        HBASE_TABLE_NAME("spark.hbase.table.name");

        private String fullname;

        Params(String value) {
            this.fullname = value;
        }

        public String getFullname() {
            return fullname;
        }
    }

    private static Map<String, String> getParams(SparkSession spark) {
        Map<String, String> params = new HashMap<>();

        Stream.of(Params.values()).forEach(p -> {
            if (!spark.conf().contains(p.fullname)) {
                throw new RuntimeException("No parameter " + p.fullname + " defined");
            }
            params.put(p.fullname, spark.conf().get(p.fullname));
        });

        return params;
    }

    private static Stream<Column> flattenSchema(Object schema, String prefix) {
        scala.collection.Iterator<StructField> iter = ((StructType) schema).iterator();
        List<StructField> all = new ArrayList<>();
        while (iter.hasNext()) {
            all.add(iter.next());
        }

        return all.stream().flatMap(field -> {
            String colName = (prefix == null) ? field.name() : prefix + "." + field.name();

            if (field.dataType() instanceof StructType) {
                return flattenSchema(field.dataType(), colName);
            } else {
                return Stream.of(col(colName));
            }
        });
    }

}
