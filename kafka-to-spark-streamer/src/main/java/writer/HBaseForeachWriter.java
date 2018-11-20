package writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.spark.sql.ForeachWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public abstract class HBaseForeachWriter<RECORD> extends ForeachWriter<RECORD> {

    protected String tableName;

    ExecutorService pool = null;
    User user = null;

    private Table hTable;
    private Connection connection;

    public HBaseForeachWriter(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public boolean open(long partitionId, long version) {
        connection = createConnection();
        hTable = getHTable(connection);
        return true;
    }

    public Connection createConnection() {
        Configuration hbaseConfig = HBaseConfiguration.create();
        Arrays.asList("hbase-site.xml").forEach(hbaseConfig::addResource);

        try {
            if (pool != null) {
                return ConnectionFactory.createConnection(hbaseConfig, pool, user);
            } else {
                return ConnectionFactory.createConnection(hbaseConfig, user);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Table getHTable(Connection connection) {
        try {
            return connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void process(RECORD record) {
        Put put = toPut(record);
        try {
            hTable.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close(Throwable errorOrNull) {
        try {
            hTable.close();
            connection.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract Put toPut(RECORD record);

}
