package crud.hbase.model;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class XxmlP {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(XxmlP.class);

	public static void main(String[] args) {
		ClassLoader cl = XxmlP.class.getClassLoader();
		InputStream inputStream = cl.getResourceAsStream("dev/hbase-site.xml");
		JAXBContext jaxbContext = null;
		try {
			jaxbContext = JAXBContext.newInstance(XmlConfiguration.class);
			Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            XmlConfiguration xml = (XmlConfiguration) jaxbUnmarshaller.unmarshal(inputStream);
            LOGGER.info(xml.toString());
            
		} catch (JAXBException e) {
			LOGGER.error("", e);
		}
		
		
		
		
		
		
		
//		InputStreamReader in = new InputStreamReader(resourceAsStream);
//		BufferedReader bf = new BufferedReader(in);
//		String n = null;
//		
//		try {
//			while ((n = bf.readLine()) != null) {
//				System.out.println(n);
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

}
