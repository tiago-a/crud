package crud.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crud.hbase.exception.ResourceFileNameOrEnvNotFound;
import crud.hbase.model.XmlConfiguration;
import crud.hbase.parser.ConfigParser;

public class App2 {

	private static final Logger LOGGER = LoggerFactory.getLogger(App2.class);

	public static void main(String[] args) {
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
			
			Scan scan = new Scan();
			scan.addFamily("event".getBytes());
			scan.setStartRow("00011230762-2019-02-26 14:38:56".getBytes());
			scan.setStopRow("00011230762-2020-02-26 16:32:44".getBytes());
			
			try {
				ResultScanner scanner = table.getScanner(scan);
				Iterator<Result> iterator = scanner.iterator();
				while(iterator.hasNext()) {
					Result next = iterator.next();
					LOGGER.info("-> " + next);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
//			long timeStamp = 1553114502172l;
//			Long timeS = new Long(timeStamp);
//			
//			BufferedMutator bufferedMutator = connection.getBufferedMutator(TableName.valueOf(Bytes.toBytes("relacionamento:teste")));
//			
////			Put p = new Put("2耗".getBytes());
//			Put p = new Put("client_id-timestamp".getBytes());
//			p.setAttribute("dados", "permissao?".getBytes());
//			p.addColumn("cf".getBytes(), "dados".getBytes(), "{info...}".getBytes());
////			Append a =  new Append("client_ID-timestamp".getBytes());
////			a.add("f".getBytes(), "dados".getBytes(), "{novas info...}".getBytes());
//
//			//			p.addColumn("f".getBytes(), "col".getBytes(), 1553112968000L, "valor".getBytes());
////			p.addColumn("f".getBytes(), "col".getBytes(), "valor".getBytes());
////			table.append(a);
//			table.put(p);
////			bufferedMutator.mutate(p);
////			bufferedMutator.flush();
//			HTableDescriptor tableDescriptor = table.getTableDescriptor();
//			LOGGER.debug("Table info: " + tableDescriptor);
//			
//			BufferedMutator.ExceptionListener listener = new ExceptionListener() {
//				
//				@Override
//				public void onException(RetriesExhaustedWithDetailsException exception, BufferedMutator mutator)
//						throws RetriesExhaustedWithDetailsException {
//					
//					
//				}
//			};
//			
//			Get get = new Get("client_id-timestamp".getBytes());
//			
//			Map<String, byte[]> attributesMap = get.getAttributesMap();
//			LOGGER.info("atrib size: " + attributesMap.size());
//			for (String key : attributesMap.keySet()) {
//				LOGGER.info("key: " + key + " value: " + new String(attributesMap.get(key), "UTF-8"));
//			}
//			
//			
////			byte[] attribute = get.getAttribute("teste");
//			byte[] attribute = "teste".getBytes();
//			String atrib = new String(attribute, "UTF-8");
//			LOGGER.info("atrib: " + atrib);
//			Authorizations authorizations = get.getAuthorizations();
//			LOGGER.info("auth: " + authorizations);
//			
////			Map<String, Object> fingerprint = get.getFingerprint();
////			for (String key : fingerprint.keySet()) {
////				LOGGER.info("key: " + key + " value: " + fingerprint.get(key).toString());
////			}
//			
//			Scan scan = new Scan();
//			scan.setBatch(10);
//			ResultScanner resultScanner = table.getScanner(scan);
////			resultScanner.iterator().hasNext()
////			for (Result result : resultScanner) {
////				LOGGER.info("result: " + result.toString());
//////				System.out.println("Rresult: " + result.toString());
//			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
