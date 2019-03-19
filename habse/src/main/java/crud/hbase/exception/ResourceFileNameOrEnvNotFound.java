package crud.hbase.exception;

public class ResourceFileNameOrEnvNotFound extends Exception {

	private String resource; 
	
	private static final long serialVersionUID = -6559787549467471431L;
	
	public ResourceFileNameOrEnvNotFound(String in) {
		this.resource = in;
	}
	
	@Override
	public String getMessage() {
		return "Nome incorreto ou inexistente: " + this.resource;
	}
	

}
