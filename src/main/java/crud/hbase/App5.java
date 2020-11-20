package crud.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crud.hbase.create.ConfResource;
import crud.hbase.create.ConnectionHbase;

public class App5 {

	private static final Logger LOGGER = LoggerFactory.getLogger(App5.class);

	private static final byte[] POSTFIX = new byte[] { 0x00 };

	public static void main(String[] args) {
		LOGGER.info("Start...");
		org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);

		Configuration setUp = ConfResource.setUp("dev");
		ConfResource.kerbLogin(setUp);
		Scanner scanIn = new Scanner(System.in);
		String decisao = "";

		String startRow = "RLRuiL10009488";
		Scan scan = new Scan();
		Filter filter = new PageFilter(25);
		scan.setFilter(filter);
		scan.setStartRow(Bytes.toBytes(startRow));

		ResultScanner scanner = null;
		Result result = null;
		try (Table connTable = ConnectionHbase.connTable("xteste", setUp)) {
			scanner = connTable.getScanner(scan);
			while (!decisao.equalsIgnoreCase("fim")) {
				result = scanner.next();
				while (result != null) {
					LOGGER.info("-> " + result);
					result = scanner.next();
				}
				LOGGER.info("?");
				decisao = scanIn.nextLine();
			}

			// while (!decisao.equalsIgnoreCase("fim")) {
			// scanner = connTable.getScanner(scan);
			// Iterator<Result> iterator = scanner.iterator();
			// while (iterator.hasNext()) {
			// Result next = iterator.next();
			// LOGGER.info("-> " + next);
			// }
			// decisao = scanIn.nextLine();
			// }
		} catch (IOException e) {
			LOGGER.error("Unexpected error", e);
		}
		scanIn.close();
	}

}
