package crud.hbase.create;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crud.hbase.exception.ResourceFileNameOrEnvNotFound;
import crud.hbase.model.XmlConfiguration;
import crud.hbase.parser.ConfigParser;

public class ConfResource {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ConfResource.class);
	
	public static Configuration setUp(String env) {
		Configuration conf = HBaseConfiguration.create();
		try {
			XmlConfiguration hbaseConf = ConfigParser.siteConf(env, "hbase-site.xml");
			XmlConfiguration coreConf = ConfigParser.siteConf(env, "core-site.xml");
			conf = ConfigParser.setConf(conf, hbaseConf);
			conf = ConfigParser.setConf(conf, coreConf);
		} catch (JAXBException | ResourceFileNameOrEnvNotFound e) {
			LOGGER.error("Unexpected error", e);
		}
		return conf;
	}
	
	public static void kerbLogin(Configuration conf) {
		String principal = "bcl_h_read@BS.BR.BSCH";
		String keytabfile = "C:/keytab/bcl_h_read.keytab";
		UserGroupInformation.setConfiguration(conf);
		try {
			UserGroupInformation.loginUserFromKeytab(principal, keytabfile);
		} catch (IOException e) {
			LOGGER.error("Unexpected error", e);
		}
	}
}
