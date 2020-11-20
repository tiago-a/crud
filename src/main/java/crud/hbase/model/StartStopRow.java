package crud.hbase.model;

public class StartStopRow {
	private String startRow;
	private String stoptRow;
	
	public StartStopRow() {	}
	
	public StartStopRow(String startRow, String stoptRow) {
		this.startRow = startRow;
		this.stoptRow = stoptRow;
	}

	public String getStartRow() {
		return startRow;
	}
	public void setStartRow(String startRow) {
		this.startRow = startRow;
	}
	public String getStoptRow() {
		return stoptRow;
	}
	public void setStoptRow(String stoptRow) {
		this.stoptRow = stoptRow;
	}
}
