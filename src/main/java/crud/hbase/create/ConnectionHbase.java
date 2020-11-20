package crud.hbase.create;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionHbase {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionHbase.class);

	public static Table connTable(String tableName, Configuration conf) throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)));
		return table;
//		try (Connection connection = ConnectionFactory.createConnection(conf);
//				Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)));) {
//		} catch (IOException e) {
//			LOGGER.error("Unexpected error", e);
//		}
//		return null;
	}

}
