package eu.anonymized.script;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

import eu.anonymized.util.ConfigYaml;



public class CreateDDL {

	
	FileWriter fw=null;

	public CreateDDL(){
		try {
			fw=new FileWriter(new File("table_DDL.sql"));
		} catch (IOException e2) {
			e2.printStackTrace();
		}
	}


	private  Connection getConnection(){

		try{
			// Get a connection
			Connection conn = DriverManager.getConnection(ConfigYaml.getInstance().getMap().get("connection").toString());
			return conn;
		}catch(Exception e ){
			e.printStackTrace();
		}
		return null;

	}


	public void createPostgresTableDDL(String table,String sourceNamespace,String targetNamespace,Optional<String> tableAlias ){

		
       Connection conn = getConnection();
		
		String targetTable=targetNamespace+"."+table;



		Statement st=null;
		
		
		BufferedWriter bw = new BufferedWriter(fw);
		try{

			bw.write("DROP TABLE IF EXISTS "+targetTable +"; ");
			bw.newLine();
			
			st=conn.createStatement();

			ResultSet rs = st.executeQuery("select * from " +sourceNamespace+"."+tableAlias.orElse(table)+ " fetch first 1 rows only with ur" );

			ResultSetMetaData rsMetaData= rs.getMetaData();


			int columnCount =rsMetaData.getColumnCount();
			String header="Create table " + targetTable+  "  ( ";
			bw.write(header);

			for (int i = 1; i <= columnCount; i++) {

				bw.write(rsMetaData.getColumnName(i) + "  ");
				String type = rsMetaData.getColumnTypeName(i) ;
//				System.out.println("Column name "+ rsMetaData.getColumnName(i) +" - Datatype "+ type);
				if(type.equals("DECIMAL") || type.equals("NUMBER")){
					bw.write("NUMERIC " + "("+ rsMetaData.getPrecision(i)+ ","+ rsMetaData.getScale(i) + ")");
				}

				if(type.equals("CHAR") || type.equals("VARCHAR")){
					bw.write("CHARACTER VARYING" + "("+ rsMetaData.getPrecision(i) + ")");
				}

				if(type.equals("DATE") || type.equals("TIME") ){
					bw.write(type );
				}
				
	
				if(type.equals("TIMESTAMP") ){
					bw.write(type + "("+ rsMetaData.getPrecision(i) + ")");
				}


				if(i!=columnCount){bw.write(",");}
			}
			bw.write(",DWH_PERIOD NUMERIC " );
			bw.write(")");
			bw.newLine();
			bw.write(" PARTITION BY RANGE (dwh_period)");
			bw.newLine();
			bw.write(";");

			bw.flush();

			bw.newLine();
			

			LocalDate start = LocalDate.of(2017, 01, 01);
			LocalDate end = LocalDate.of(2021,12,01);


			for(LocalDate data=start;data.isBefore(end)|| data.isEqual(end);data=data.plusMonths(1)){
				int currentMonth=Integer.parseInt(data.format(DateTimeFormatter.ofPattern("yyyyMM")));
				String script=String.format("CREATE TABLE IF NOT EXISTS %s PARTITION OF %s  FOR VALUES FROM (%s) TO (%s);", 
						targetNamespace+"."+table+"_"+currentMonth,
						targetNamespace+"."+table,
						currentMonth,
						currentMonth+1);
				bw.write(script);
				bw.newLine();
			}

			bw.flush();
			
		}catch(Exception e ){
			e.printStackTrace();
			throw new RuntimeException(e);
		}finally{

			try{
				st.close();
				conn.close();
				st=null;
			}catch(Exception e){e.printStackTrace();}
			}

//		try {
//			fw.close();
//		} catch (IOException e1) {
//
//			e1.printStackTrace();
//		}
//


	}








	public static void main(String[] args) {
		try {
			System.out.println("Start creating ddl........");
	        if(args==null || args.length<1){
	        	System.out.println("Param 1 - Configuration file");
	        	System.out.println("End with error");
	        	System.exit(0);
	        }
			
			ConfigYaml.initConfig(args[0]);
			List<String> listaTabelle= ConfigYaml.getInstance().getTables();
			
			CreateDDL c = new CreateDDL();
			listaTabelle.forEach(tabella->{
				System.out.println("Tabella " + tabella);
				try {
					c.createPostgresTableDDL(tabella.toLowerCase(),ConfigYaml.getInstance().getTable(tabella).get("namespace").toString(),"public",
							Optional.of( ConfigYaml.getInstance().getTable(tabella).get("query").toString()));
				}catch(Exception sq) {
					sq.printStackTrace();
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});


			System.exit(0);
		} catch (Exception e) {

			e.printStackTrace();
		}finally {
			
		}


	}

}

