package crud.hbase;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crud.hbase.create.ConfResource;
import crud.hbase.create.ConnectionHbase;
import crud.hbase.query.MyScans;

public class App4 {

	private static final Logger LOGGER = LoggerFactory.getLogger(App4.class);

	public static void main(String[] args) {
		LOGGER.info("Start...");
		org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);

		Configuration setUp = ConfResource.setUp("dev");
		ConfResource.kerbLogin(setUp);
		
		int loop = 5;
//		int loop = 100;
		
		long[] tempo = new long[loop]; 
		
//		Scan scan = MyScans.ex001();
//		Scan scan = MyScans.ex002();
//		Scan scan = MyScans.ex003();
//		Scan scan = MyScans.ex004();
//		Scan scan = MyScans.ex005();
		Scan scan = MyScans.ex006();
		scan.setScanMetricsEnabled(true);
		for(int i = 0; i < loop; i++) {
			ResultScanner scanner = null;

			//ex001(), ex002(), ex003();
//			try (Table connTable = ConnectionHbase.connTable("relacionamento:timeline_v2", setUp)) {
				
			//ex004();
			try (Table connTable = ConnectionHbase.connTable("xteste", setUp)) {
			
				long totalSize = 0l;
				scanner = connTable.getScanner(scan);
				Iterator<Result> iterator = scanner.iterator();
				long inicio = System.currentTimeMillis();
//				Result[] next2 = scanner.next(99999);
//				for (int j = 0; j < next2.length ; j++) {
////					LOGGER.info("next2: " + next2[j]);
//				}
				while (iterator.hasNext()) {
					Result next = iterator.next();
//					CellScanner cellScanner = next.cellScanner();
//					while(cellScanner.advance()) {
//						Cell current = cellScanner.current();
//						LOGGER.info("heapSize: " + CellUtil.estimatedHeapSizeOf(current));
//					}
					
					//Tamanho total em bytes retornado do Scanner
					totalSize += Result.getTotalSizeOfCells(next);//Retorna o total em bytes de todas 'Cells' contidas num único 'Result' 
							
//					LOGGER.info("-> " + next);
				}
				//teste para rowkey
//				String md5AsHex = MD5Hash.getMD5AsHex("aaa".getBytes());
//				LOGGER.info("---> " + md5AsHex);
				LOGGER.info("size: " + totalSize);
				long fim = System.currentTimeMillis();
				long total = fim - inicio;
				tempo[i] = total;
				LOGGER.info("Total time: " + total +"ms");
			} catch (IOException e) {
				LOGGER.error("Unexpected error", e);
			}
			ScanMetrics metrics = scan.getScanMetrics();
			LOGGER.info("# of Rows Scanned: " + metrics.countOfRowsScanned);
			LOGGER.info("# of RPC Calls: " + metrics.countOfRPCcalls);
			LOGGER.info("Sum of milisec between nexts: " + metrics.sumOfMillisSecBetweenNexts);
			scanner.close();
		}
		long totalTime = 0l;
		for(int i = 0; i < loop; i++) {
			totalTime += tempo[i];
		}
		LOGGER.info("Média: " + totalTime/loop + "ms");
		
	}
}
