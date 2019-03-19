package crud.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App 
{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
	
    public static void main( String[] args )
    {
    	org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN);

		LOGGER.info("Starting");
//		System.out.println("Starting...");
		if(args.length >= 1) {
			System.out.println("args: " + args[0]);
		}
		
		
		ClassLoader a = App.class.getClassLoader();
		System.out.println(a.getResource("testing/hbase/CrudHbase.class"));
		System.out.println(a.getResource("org/apache/hadoop/hbase/HBaseConfiguration.class"));
		Configuration conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.quorum", "bdbdrdlbssbr013.bs.br.bsch,bdbdrdlbnibr004.bs.br.bsch,bdbdrdlbssbr005.bs.br.bsch");
		conf.set("hbase.rootdir", "hdfs://bdbdrdlbssbr006.bs.br.bsch:8020/hbase");
		conf.set("hbase.client.write.buffer", "2097152");
		conf.set("hbase.client.pause", "100");
		conf.set("hbase.client.retries.number", "35");
		conf.set("hbase.client.scanner.caching", "100");
		conf.set("hbase.client.keyvalue.maxsize", "10485760");
		conf.set("hbase.ipc.client.allowsInterrupt", "true");
		conf.set("hbase.client.primaryCallTimeout.get", "10");
		conf.set("hbase.client.primaryCallTimeout.multiget", "10");
		conf.set("hbase.fs.tmp.dir", "/user/${user.name}/hbase-staging");
		conf.set("hbase.client.scanner.timeout.period", "60000");
		conf.set("hbase.coprocessor.master.classes", "org.apache.hadoop.hbase.security.access.AccessController");
		conf.set("hbase.coprocessor.region.classes", "org.apache.hadoop.hbase.security.access.AccessController,org.apache.hadoop.hbase.security.token.TokenProvider,org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint");
		conf.set("hbase.regionserver.thrift.http", "true");
		conf.set("hbase.thrift.ssl.enabled", "false");
		conf.set("hbase.thrift.support.proxyuser", "true");
		conf.set("hbase.rpc.timeout", "60000");
		conf.set("hbase.snapshot.enabled", "true");
		conf.set("hbase.snapshot.master.timeoutMillis", "60000");
		conf.set("hbase.snapshot.region.timeout", "60000");
		conf.set("hbase.snapshot.master.timeout.millis", "60000");
		conf.set("hbase.security.authentication", "kerberos");
		conf.set("hbase.rpc.protection", "authentication");
		conf.set("zookeeper.session.timeout", "120000");
		conf.set("zookeeper.znode.parent", "/hbase");
		conf.set("zookeeper.znode.rootserver", "root-region-server");
		conf.set("hbase.zookeeper.quorum", "bdbdrdlbssbr013.bs.br.bsch,bdbdrdlbnibr004.bs.br.bsch,bdbdrdlbssbr005.bs.br.bsch");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hadoop.security.authorization", "true");
		conf.set("hbase.rest.ssl.enabled", "false");
		conf.set("hadoop.security.authentication", "kerberos");
		
		String principal = "bcl_h_read@BS.BR.BSCH";
		String keytabfile = "C:/keytab/bcl_h_read.keytab";
		
		conf.set("hbase.zookeeper.kerberos.principal", principal);
//		conf.set("hbase.zookeeper.kerberos.principal", "hbase/_HOST@BS.BR.BSCH");
		conf.set("hbase.zookeeper.keytab.file", keytabfile);
		conf.set("hbase.regionserver.keytab.file", keytabfile);
		conf.set("hbase.master.keytab.file", keytabfile);
//		conf.set("hbase.master.kerberos.principal", principal);  
		conf.set("hbase.master.kerberos.principal", "hbase/_HOST@BS.BR.BSCH");
//		conf.set("hbase.regionserver.kerberos.principal", principal);
		conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@BS.BR.BSCH");
//		conf.set("hbase.rest.kerberos.principal", principal);        
		conf.set("hbase.rest.kerberos.principal", "hbase/_HOST@BS.BR.BSCH");
//		conf.set("hbase.thrift.kerberos.principal", principal);
		conf.set("hbase.thrift.kerberos.principal", "HTTP/_HOST@BS.BR.BSCH");
//		conf.set("java.security.krb5.realm", "BS.BR.BSCH");
//		conf.set("java.security.krb5.kdc", "dcbs31.bs.br.bsch:88");
		conf.set("hadoop.registry.kerberos.realm ", "BS.BR.BSCH");
		conf.set("java.security.krb5.conf","C:/keytab/kbr5.conf");
		StringBuilder sb = new StringBuilder();
		sb.append("RULE:[1:$1]/L").append("\n");
		sb.append("RULE:[1:$1@$0](.*@\\\\QBS.BR.BSCH\\\\E$)s/@\\\\QBS.BR.BSCH\\\\E$//").append("\n");
		sb.append("RULE:[2:$1@$0](.*@\\QBS.BR.BSCH\\E$)s/@\\QBS.BR.BSCH\\E$//").append("\n");
		sb.append("DEFAULT");
		String rule = sb.toString();
		conf.set("hadoop.security.auth_to_local", rule);

		UserGroupInformation.setConfiguration(conf);
		try {
			UserGroupInformation.loginUserFromKeytab(principal, keytabfile);
			System.out.println("short-name: " + UserGroupInformation.getCurrentUser().getShortUserName());
			System.out.println("User: " + UserGroupInformation.getLoginUser().getUserName());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		UserProvider instantiate = UserProvider.instantiate(conf);
		User user = null;
		try {
			user = instantiate.getCurrent();
			System.out.println("user rpc: " + user.getName());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		User user = RpcServer.getRequestUser();
//		UserGroupInformation.getLoginUser().getRealUser().user
//		User user = new User();
//		String string = conf.get("hbase.zookeeper.quorum");
//		System.out.println(string);

		try (
//				Connection connection = ConnectionFactory.createConnection(conf, user);
				Connection connection = ConnectionFactory.createConnection(conf);
				Table table = connection.getTable(TableName.valueOf(Bytes.toBytes("relacionamento:teste")))) {
			System.out.println("daasdasddadas");
//			HTableDescriptor tableDescriptor = table.getTableDescriptor();
//			System.out.println("tableDescriptor: " + tableDescriptor.toString());
			Put put = new Put("henrique".getBytes());
			
//			byte[] data = new byte[100];
//			String username = "rafael";
//			byte[] username_byte = username.getBytes(Charset.forName("UTF8"));
//			System.out.println("username_byte length: " + username_byte.length);
//			System.arraycopy(username_byte, 0, data, 45, username_byte.length);
//			Put put = new Put(data, 45, username_byte.length);
			put.addColumn("cf".getBytes(), "col".getBytes(), "100e21e12e1200".getBytes());
//			
			table.put(put);
//			Get get = new Get("relacionamento:transacao".getBytes());
//			Result result = new Result();
//			result = table.get(get);
//			
//			System.out.println("result: " + result.toString());
			
//			Scan scan = new Scan();
//			ResultScanner resultScanner = table.getScanner(scan);
//			for (Result result : resultScanner) {
//				LOGGER.info("result: " + result.toString());
////				System.out.println("Rresult: " + result.toString());
//			}
			
			
//			while(resultScanner.iterator().hasNext()) {
//				Result next = resultScanner.iterator().next();
//				System.out.println("scan: " + next.toString());
//			}
			
			

		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
