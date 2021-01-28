package eu.anonymized;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import eu.anonymized.util.RetryException;
import eu.anonymized.crypto.CryptoUtils;

public class Dump<R> implements Function<ParameterBean,R> {

	private static final Logger LOG = LoggerFactory.getLogger(Dump.class);
	private Predicate<File> canDownload;

	public Dump(Predicate<File> p) {
		this.canDownload=p;
	}

	@Override
	public R apply(ParameterBean params)  {
		Connection conn=null;
		try{
			File check = new File(params.getDirectory()+"/"+params.getTable()+"."+Constants.FILE_DOWNLOADED);
			if(!canDownload.test(check)) {
				LOG.info("Table {} already dumped. Skip.....",params.getTable());
				return null;
			}

			//String passwordDB=CryptoUtils.decryptAES(params.getPassword());

			// Creating connection  
			//jdbc:oracle:thin:tiger/scott@localhost:1521:productDB
			conn=DriverManager.getConnection(params.getConnectionString());  
			LOG.info("Init dump table {}  at {}",params.getTable(),LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
			downloadTable(params.getTable(), params.getQuery(),conn, params.getDirectory(),params.getSecretKey());
			LOG.info("End dump table {} at {}",params.getTable(),LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
			// Closing connection  
			conn.close();    
		/*}catch(DisconnectNonTransientConnectionException de){
			throw new RetryException(de);*/
		}catch(Exception e){
			LOG.error("Cannot dump table {}",params.getTable(),e);
		}finally{
			if(conn!=null)
				try {
					conn.close();
				} catch (SQLException e) {
				}
		}

		return null;
	}

	private  void downloadTable(String table,String query,Connection conn,String directory,String secretKey64) 
			throws Exception{




		Statement stmt=conn.createStatement();   
		LOG.debug("SecretKey 64 {}",secretKey64);
		String secretKeyDecrypted=CryptoUtils.decryptAES(secretKey64);
		LOG.debug("SecretKey decrypted {}",secretKeyDecrypted);
		ResultSet rs=stmt.executeQuery(query); 
		int columnCount=rs.getMetaData().getColumnCount();
		File f = new File(directory+"/"+table+"."+System.currentTimeMillis());

		FileOutputStream fos = new FileOutputStream(f);

		while(rs.next()){
			StringBuffer buf = new StringBuffer();
			for (int i = 1; i <= columnCount; i++) {
				try{
					String tmp=rs.getString(i);
					tmp=tmp.replaceAll(Constants.FIELD_DELIMITER, "");
					if(tmp.trim().length()>0)
						tmp=tmp.trim();
					buf.append(tmp);
				}catch(Exception e){
					//            			e.printStackTrace();
				}
				if(i<columnCount)
					buf.append(Constants.FIELD_DELIMITER);
				
				
			}
			
			String riga=buf.toString().replaceAll("\\u0000", "");
			LOG.debug("Riga {}",riga);
			//riga= CryptoUtils.encryptAES(riga, secretKeyDecrypted);
			LOG.debug("Riga crypted " + riga);

			fos.write(riga.getBytes());
			fos.write("\r\n".getBytes());
			fos.flush();
		}  
		stmt.close();
		fos.close();
		File check = new File(directory+"/"+table+"."+Constants.FILE_DOWNLOADED);
		f.renameTo(check);

	}


}
