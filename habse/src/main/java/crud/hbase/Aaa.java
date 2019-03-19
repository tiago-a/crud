package crud.hbase;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Aaa {
	
	private static final String MAP = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final long BASE_62 = 62L;
    private static final Logger LOGGER = LoggerFactory.getLogger(Aaa.class);

	public static void main(String[] args) {
		org.apache.log4j.Logger.getLogger("crud").setLevel(Level.DEBUG);
//		for (int i = 33; i <= 65535; i++) {
//			char unicode = (char) i;
//			System.out.print(unicode);
//		}
//		StringBuilder base62 = base62(6990590000123L);
		StringBuilder base62 = base62(2372360108L);
		LOGGER.debug("base62: " + base62.toString());
	}
	
	 public static StringBuilder base62(final long num) {
	        long q = num < 0 ? num * -1 : num, r;
	        LOGGER.debug("q: " + q);
	        StringBuilder res = new StringBuilder();
	        while (q != 0) {
	            r = q % BASE_62;
	            LOGGER.debug("r: " + r);
	            q /= BASE_62;
	            LOGGER.debug("q: " + q);
	            r = (r < 0 ? r + BASE_62 : r);
	            res.append(MAP.charAt((int) r));
	            LOGGER.debug("MAP: " + MAP.charAt((int) r));
	            LOGGER.debug("end");
	        }
	        return res.reverse();
	    }

}
