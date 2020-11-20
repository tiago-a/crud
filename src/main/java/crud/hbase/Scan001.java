package crud.hbase;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crud.hbase.exception.ResourceFileNameOrEnvNotFound;
import crud.hbase.model.XmlConfiguration;
import crud.hbase.parser.ConfigParser;

public class Scan001 {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Scan001.class);

	public static void main(String[] args) {
		LOGGER.info("Startin...");
		org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);
		
		Configuration conf = HBaseConfiguration.create();
		try {
			XmlConfiguration hbaseConf = ConfigParser.siteConf("hk", "hbase-site.xml");
			XmlConfiguration coreConf = ConfigParser.siteConf("hk", "core-site.xml");
			conf = ConfigParser.setConf(conf, hbaseConf);
			conf = ConfigParser.setConf(conf, coreConf);
			LOGGER.debug(conf.toString());
		} catch (JAXBException | ResourceFileNameOrEnvNotFound e) {
			LOGGER.error("Unexpected error", e);
		}
		String principal = "bcl_h_read@BS.BR.BSCH";
		String keytabfile = "C:/keytab/bcl_h_read.keytab";
		UserGroupInformation.setConfiguration(conf);
		try {
			UserGroupInformation.loginUserFromKeytab(principal, keytabfile);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		try (Connection connection = ConnectionFactory.createConnection(conf);
				Table table = connection.getTable(TableName.valueOf(Bytes.toBytes("relacionamento:timeline")))) {
			
			Get get = new Get("00011230762-2019-02-26 14:38:56-loginmobile".getBytes());
			Result result = table.get(get);
			LOGGER.info(result.toString());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
