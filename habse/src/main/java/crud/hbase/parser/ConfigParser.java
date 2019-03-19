package crud.hbase.parser;

import java.io.InputStream;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crud.hbase.exception.ResourceFileNameOrEnvNotFound;
import crud.hbase.model.XmlConfiguration;
import crud.hbase.model.XmlProperty;

public class ConfigParser {

	private static final Logger LOGGER = LoggerFactory.getLogger(ConfigParser.class);

	public static XmlConfiguration siteConf(String env, String resourceFileName) throws JAXBException, ResourceFileNameOrEnvNotFound {
		LOGGER.debug("resourceFileName: " + resourceFileName);
		LOGGER.debug("env: " + env);
		StringBuilder sb = new StringBuilder();
		sb.append(env).append("/").append(resourceFileName);
		String resource = sb.toString();

		ClassLoader classLoader = ConfigParser.class.getClassLoader();
		InputStream resourceAsStream = classLoader.getResourceAsStream(resource);
		if (resourceAsStream == null) {
			throw new ResourceFileNameOrEnvNotFound(resource);
		}
		JAXBContext jaxbContext = null;
		jaxbContext = JAXBContext.newInstance(XmlConfiguration.class);
		Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		XmlConfiguration xml = (XmlConfiguration) jaxbUnmarshaller.unmarshal(resourceAsStream);
		LOGGER.debug("xml content: " + xml);
		return xml;
	}
	
	public static Configuration setConf(XmlConfiguration xml) {
		Configuration conf = HBaseConfiguration.create();
		List<XmlProperty> properties = xml.getProperties();
		for (XmlProperty property : properties) {
            if (property.getName() != null || property.getValue() != null) {
                conf.set(property.getName(), property.getValue());
            }

        }
		return conf;
	}
}
