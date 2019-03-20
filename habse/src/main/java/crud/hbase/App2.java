package crud.hbase;

import javax.xml.bind.JAXBException;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crud.hbase.exception.ResourceFileNameOrEnvNotFound;
import crud.hbase.model.XmlConfiguration;
import crud.hbase.parser.ConfigParser;

public class App2 {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(App2.class);

	public static void main(String[] args) {
		try {
//			XmlConfiguration siteConf = ConfigParser.siteConf("dev", "hbase-site.xml");
			XmlConfiguration siteConf = ConfigParser.siteConf("dev", "dummy-file.xml");
			Configuration setConf = ConfigParser.setConf(siteConf);
			LOGGER.debug(setConf.toString());

//			LOGGER.debug("xml content: " + siteConf.toString());
		} catch (JAXBException | ResourceFileNameOrEnvNotFound e) {
			LOGGER.error("Unexpected error", e);
			// TODO Auto-generated catch block
//			e.printStackTrace();
		} 
	}

}
