package eu.anonymized;

import java.util.Optional;

public class ParameterBean {

	private String table;
	private String directory;
	private String secretKey;
	private String user;
	private String password;
	private String connectionString;
	private String query;
	
	
	public ParameterBean() {
	}
	
	
	
	/**
	 * @param table
	 * @param directory
	 * @param secretKey
	 * @param user
	 * @param password
	 * @param connectionString
	 */
	public ParameterBean(String table,String query, String directory, String connectionString) {
		super();
		this.table = table;
		this.directory = directory;
		//this.secretKey = secretKey;
		this.connectionString=connectionString;
		this.query=query;
		
		
	}
	
	public String getQuery() {
		return query;
	}



	public void setQuery(String query) {
		this.query = query;
	}


/*
	public ParameterBean(String table, String directory, String secretKey, String user, String password) {
		super();
		this.table = table;
		this.directory = directory;
		this.secretKey = secretKey;
		this.user = user;
		this.password = password;
		
	}
*/



	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public String getDirectory() {
		return directory;
	}
	public void setDirectory(String directory) {
		this.directory = directory;
	}
	public String getSecretKey() {
		return secretKey;
	}
	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}

	

	public String getConnectionString() {
		return connectionString;
	}



	public void setConnectionString(String connectionString) {
		this.connectionString = connectionString;
	}

	
	

}
