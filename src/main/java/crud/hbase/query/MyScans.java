package crud.hbase.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyScans {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MyScans.class);

	public static Scan ex001() {
		String cf = "e";
		String startRow = "00NKPBx2-9DcXP0";
		String stopRow = "6yzTTBtA-9DdaDx";
		String table = "relacionamento:timeline_v2";
		
		LOGGER.info("column family set to '" + cf + "'");
		LOGGER.info("all columns qualifiers set");
		LOGGER.info("Start row: " + startRow);
		LOGGER.info("Stop row: " + stopRow);
		LOGGER.info("table: " + table);
		
		Scan scan = new Scan();
		scan.addColumn(cf.getBytes(), "channel".getBytes());
		scan.addColumn(cf.getBytes(), "contract".getBytes());
		scan.addColumn(cf.getBytes(), "date".getBytes());
		scan.addColumn(cf.getBytes(), "document".getBytes());
		scan.addColumn(cf.getBytes(), "hashId".getBytes());
		scan.addColumn(cf.getBytes(), "penumper".getBytes());
		scan.addColumn(cf.getBytes(), "source".getBytes());
		scan.addColumn(cf.getBytes(), "sourceId".getBytes());
		scan.addColumn(cf.getBytes(), "subtitleType".getBytes());
		scan.addColumn(cf.getBytes(), "subtitleValue".getBytes());
		scan.addColumn(cf.getBytes(), "titleType".getBytes());
		scan.addColumn(cf.getBytes(), "titleValue".getBytes());
		scan.addColumn(cf.getBytes(), "type".getBytes());
		scan.addColumn(cf.getBytes(), "typeId".getBytes());
		scan.addFamily(cf.getBytes());
		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());
		return scan;
	}
	
	public static Scan ex002() {
		String cf = "e";
		String startRow = "00NKPBx2-9DcXP0";
		String stopRow = "6yzTTBtA-9DdaDx";
		String table = "relacionamento:timeline_v2";
		
		LOGGER.info("column family set to '" + cf + "'");
		LOGGER.info("columns qualifiers not set");
		LOGGER.info("Start row: " + startRow);
		LOGGER.info("Stop row: " + stopRow);
		LOGGER.info("table: " + table);
		
		Scan scan = new Scan();
		scan.addFamily(cf.getBytes());
		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());
		return scan;
	}
	
	public static Scan ex003() {
		String cf = "e";
		String table = "relacionamento:timeline_v2";
		
		LOGGER.info("column family set to '" + cf + "'");
		LOGGER.info("columns qualifiers not set");
		LOGGER.info("No start row");
		LOGGER.info("No stop row");
		LOGGER.info("table: " + table);
		
		Scan scan = new Scan();
		scan.addFamily(cf.getBytes());
		return scan;
	}
	
	public static Scan ex004() {
		String cf = "e";
		
//		String startRow = "HuuYAW10126752";
//		String stopRow = "HuuYAW11778999";
		String startRow = "RLRuiL10009488";
		String stopRow = "RLRuiL99984205";
//		String startRow = "RY23iG10038553";
//		String stopRow = "RY23iG99983007";
//		String startRow = "0ayFzh10561792";
//		String stopRow = "0ayFzh99850261";
//		String startRow = "05HMpi10074971";
//		String stopRow = "05HMpi99982174";
		String table = "xteste";
		
		LOGGER.info("column family set to '" + cf + "'");
		LOGGER.info("all columns qualifiers set");
		LOGGER.info("Start row: " + startRow);
		LOGGER.info("Stop row: " + stopRow);
		LOGGER.info("table: " + table);
		Scan scan = new Scan();
		scan.addColumn(cf.getBytes(), "d".getBytes());
//		scan.addFamily(cf.getBytes());
		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());
		scan.setMaxResultsPerColumnFamily(1);
		scan.setBatch(1);
		scan.setCaching(100);
		LOGGER.info("getCaching: " + scan.getCaching());
		return scan;
	}
	
	public static Scan ex005() {
		String cf = "e";
		
//		String startRow = "HuuYAWi5-9E5q85";
//		String stopRow = "HuuYAWi5-9E5qCP";
//		String startRow = "RLRuiLRA-9E5o54";
//		String stopRow = "RLRuiLRA-9E6pRF";
//		String startRow = "RY23iGUy-9E5ocL";
//		String stopRow = "RY23iGUy-9E6pOE";	
//		String startRow = "0ayFzhOM-9E5p7R";
//		String stopRow = "0ayFzhOM-9E6fNR";		
		String startRow = "05HMpiR6-9Dvazz";
		String stopRow = "05HMpiR6-9E6onV";
		String table = "xteste";
		
		LOGGER.info("column family set to '" + cf + "'");
		LOGGER.info("all columns qualifiers set");
		LOGGER.info("Start row: " + startRow);
		LOGGER.info("Stop row: " + stopRow);
		LOGGER.info("table: " + table);
		Scan scan = new Scan();
		scan.addColumn(cf.getBytes(), "channel".getBytes());
		scan.addColumn(cf.getBytes(), "contract".getBytes());
		scan.addColumn(cf.getBytes(), "date".getBytes());
		scan.addColumn(cf.getBytes(), "document".getBytes());
		scan.addColumn(cf.getBytes(), "hashId".getBytes());
		scan.addColumn(cf.getBytes(), "penumper".getBytes());
		scan.addColumn(cf.getBytes(), "source".getBytes());
		scan.addColumn(cf.getBytes(), "sourceId".getBytes());
		scan.addColumn(cf.getBytes(), "subtitleType".getBytes());
		scan.addColumn(cf.getBytes(), "subtitleValue".getBytes());
		scan.addColumn(cf.getBytes(), "titleType".getBytes());
		scan.addColumn(cf.getBytes(), "titleValue".getBytes());
		scan.addColumn(cf.getBytes(), "type".getBytes());
		scan.addColumn(cf.getBytes(), "typeId".getBytes());
//		scan.addFamily(cf.getBytes());
		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());
		scan.setMaxResultsPerColumnFamily(14);
//		scan.setCaching(999999);
//		LOGGER.info("getCaching: " + scan.getCaching());
		return scan;
	}
	
	public static Scan ex006() {
		Scan scan = new Scan();
		String cf = "e";
		
		String startRow = "05HMpiR6-9Dvazz";
		String prefixRow = "05HMpiR6-9";
		String stopRow = "05HMpiR6-9E6onV";
		
		List<Filter> filters = new ArrayList<Filter>();
		Filter filterStart = new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(startRow)));
		filters.add(filterStart);
		Filter filterEnd = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(stopRow)));
		filters.add(filterEnd);
//		Filter prefix = new PrefixFilter(Bytes.toBytes(prefixRow));
		scan.addFamily(Bytes.toBytes(cf));
//02372360108-2019-01-23 19:45:10
//02372360108-2019-01-01 14:12:28		
//		scan.setFilter(prefix);
//		scan.setFilter(filterEnd);
		FilterList filterList = new FilterList(filters);
		scan.setFilter(filterList);
		scan.addColumn(cf.getBytes(), "channel".getBytes());
		scan.addColumn(cf.getBytes(), "contract".getBytes());
		scan.addColumn(cf.getBytes(), "date".getBytes());
		scan.addColumn(cf.getBytes(), "document".getBytes());
		scan.addColumn(cf.getBytes(), "hashId".getBytes());
		scan.addColumn(cf.getBytes(), "penumper".getBytes());
		scan.addColumn(cf.getBytes(), "source".getBytes());
		scan.addColumn(cf.getBytes(), "sourceId".getBytes());
		scan.addColumn(cf.getBytes(), "subtitleType".getBytes());
		scan.addColumn(cf.getBytes(), "subtitleValue".getBytes());
		scan.addColumn(cf.getBytes(), "titleType".getBytes());
		scan.addColumn(cf.getBytes(), "titleValue".getBytes());
		scan.addColumn(cf.getBytes(), "type".getBytes());
		scan.addColumn(cf.getBytes(), "typeId".getBytes());
//		scan.setStartRow(startRow.getBytes());
//		scan.setStopRow(stopRow.getBytes());
		return scan;
	}
	
	public static Scan ex007() {
		String cf = "e";
		
//		String startRow = "HuuYAW10126752";
//		String stopRow = "HuuYAW11778999";
		String startRow = "RLRuiL10009488";
		String stopRow = "RLRuiL99984205";
//		String startRow = "RY23iG10038553";
//		String stopRow = "RY23iG99983007";
//		String startRow = "0ayFzh10561792";
//		String stopRow = "0ayFzh99850261";
//		String startRow = "05HMpi10074971";
//		String stopRow = "05HMpi99982174";
		String table = "xteste";
		
		LOGGER.info("column family set to '" + cf + "'");
		LOGGER.info("all columns qualifiers set");
		LOGGER.info("Start row: " + startRow);
		LOGGER.info("Stop row: " + stopRow);
		LOGGER.info("table: " + table);
		Filter filter = new ColumnPaginationFilter(10, 0);
		Scan scan = new Scan();
//		scan.setFilter(filter);
		scan.addColumn(cf.getBytes(), "d".getBytes());
		scan.addFamily(cf.getBytes());
		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());
//		scan.setMaxResultsPerColumnFamily(1);
//		scan.setBatch(1);
//		scan.setCaching(100);
//		LOGGER.info("getCaching: " + scan.getCaching());
		return scan;
	}

}
