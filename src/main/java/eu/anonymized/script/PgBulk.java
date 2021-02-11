package eu.anonymized.script;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.yaml.snakeyaml.Yaml;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;

import eu.anonymized.Constants;



public class PgBulk {

	private static Map<String, Object> config=null;
	private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMM");
	//private static final String truncateTemplate="su postgres -c 'psql -d database -U user -c \"drop table %s; create table %s partition of %s for values from (%s) to (%s);  \"' ";
	private final static String bashTemplate="su postgres -c '/usr/pgsql-13/bin/pg_bulkload -d %s  %s.ctl '"; 

	public static void main(String[] args) {
		
		if(args==null || args.length<1) {
			System.out.println("Arg1: config file");
			
			System.exit(0);
		}
		
		InputStream inputStream;
		try {
			Yaml yaml = new Yaml();
			inputStream = new FileInputStream(args[0]);
			config = yaml.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(1);
		}

		

		new PgBulk().run();
		System.exit(0);

	}

	public void run() {
		String dirIn = config.get("sourcedir").toString();
		
		String dirOut= config.get("targetdir").toString();

		List<String> listTemplate=new ArrayList<String>();
		try {
			if(dirOut.endsWith("/"))dirOut=dirOut.substring(0, dirOut.lastIndexOf("/"));
			File dir = new File(dirIn);
			MustacheFactory mf = new DefaultMustacheFactory();
			Mustache mustache = mf.compile("pgbulk.mustache");
			

			 
			if(dir.isDirectory()){
				File[] ff = dir.listFiles();
				for (int i = 0; i < ff.length; i++) {
					if(ff[i].getName().startsWith("."))continue;
					if(!ff[i].getName().endsWith(Constants.FILE_DOWNLOADED))continue;
					FileWriter fw = new FileWriter(dirOut+"/"+ff[i].getName()+".ctl");
					Model m = new PgBulk.Model(ff[i].getAbsolutePath(), 
							config.get("user").toString(), 
							config.get("password").toString(),
							config.get("host").toString(), 
							config.get("port").toString(), 
							config.get("database").toString(),
							config.get("namespace").toString(), 
							retrieveParentTable(ff[i].getName()), 
							retrieveParentTable(ff[i].getName()), 
							retrieveStratingPartition(ff[i].getName()), 
							retrieveEndPartition(ff[i].getName()));
					
					mustache.execute(fw, m).flush();
					
					/*listTemplate.add(
							String.format(truncateTemplate, 
									m.schema+"."+m.table,
									m.schema+"."+m.table,
									m.schema+"."+m.parent_table,
									m.starting_partition,
									m.end_partition
									)
							);*/
					listTemplate.add(String.format(bashTemplate,config.get("database").toString(),ff[i].getName()));
					 

				}
			}else{
				FileWriter fw = new FileWriter(dirOut+"/"+dir.getName()+".ctl");
				Model m = new PgBulk.Model(dir.getAbsolutePath(), 
						config.get("user").toString(), 
						config.get("password").toString(),
						config.get("host").toString(), 
						config.get("port").toString(), 
						config.get("database").toString(),
						config.get("namespace").toString(),  
						retrieveParentTable(dir.getName()), 
						retrieveParentTable(dir.getName()), 
						retrieveStratingPartition(dir.getName()), 
						retrieveEndPartition(dir.getName()));
				
				mustache.execute(fw, m).flush();
				/*
				listTemplate.add(
						String.format(truncateTemplate, 
								m.schema+"."+m.table,
								m.schema+"."+m.table,
								m.schema+"."+m.parent_table,
								m.starting_partition,
								m.end_partition
								)
						);*/
				listTemplate.add(String.format(bashTemplate,config.get("database").toString(),dir.getName()));
				 

			}


			FileWriter fw = new FileWriter(dirOut+"/bulk.sh");
			listTemplate.forEach(t->{
				try {
					fw.write("***********************************************"+"\n");
					fw.write(t+"\n");
					fw.write("\n");
					fw.write("echo 'Running command: [" + t + "]'");
					fw.write("\n");
					fw.write("***********************************************"+"\n");
					fw.write("\n");					
					fw.write("\n");
					fw.write("\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
			fw.flush();
			fw.close();
			
			

		} catch (Exception e) {
			e.printStackTrace();
		}
	}




	private String retrieveTableName(String fileName) {
	
		Pattern p = Pattern.compile(Constants.DATA_NAME_PATTERN);
		Matcher m = p.matcher(fileName);
		
		if(m.find()) {
			String table=fileName.substring(fileName.indexOf(".")+1,fileName.indexOf(m.group()));
			return table+"_"+m.group();
		}else {
			return fileName.split("\\.")[1]+"_"+ LocalDate.now().format(dtf);
		}
	}

	private String retrieveParentTable(String fileName) {
		System.out.println("File name " + fileName);
		Pattern p = Pattern.compile(Constants.DATA_NAME_PATTERN);
		Matcher m = p.matcher(fileName);
		if(m.find()) {
			return fileName.substring(fileName.indexOf(".")+1,fileName.indexOf(m.group()));
		}else {
			return fileName.split("\\.")[1];
		}

	}

	private String retrieveStratingPartition(String fileName) {
		Pattern p = Pattern.compile(Constants.DATA_NAME_PATTERN);
		Matcher m = p.matcher(fileName);
		if(m.find()) {
			String data=m.group();
			return data.substring(0, 4) + data.substring(4);
		}else {
			System.out.println(LocalDate.now().format(dtf));
			return LocalDate.now().format(dtf);
		}

	}

	private String retrieveEndPartition(String fileName) {
		Pattern p = Pattern.compile(Constants.DATA_NAME_PATTERN);
		Matcher m = p.matcher(fileName);
		if(m.find()) {
			String data=m.group();
			return data.substring(0, 4) + String.format("%02d", Integer.parseInt(data.substring(4))+1);
		}else {
			return LocalDate.now().plusMonths(1).format(dtf);
		}
	}

//	public Model fillNew() {
//		return new Model("", "crypto", "crypto", "172.16.33.98", "5432", "grafo", "public", "","", "", "");
//	}
//

	public class Item {
		List<Model> items =new ArrayList<PgBulk.Model>();
	}

	public class Model {
		String file_name;
		String user_name;
		String pwd;
		String host;
		String port;
		String database;
		String schema;
		String table;
		String parent_table;
		String starting_partition;
		String end_partition;
		String delimiter="	";

		public Model(String file_name, String user_name, String pwd, String host, String port, String database,
				String schema, String table, String parent_table, String starting_partition, String end_partition) {
			super();
			this.file_name = file_name;
			this.user_name = user_name;
			this.pwd = pwd;
			this.host = host;
			this.port = port;
			this.database = database.toLowerCase();
			this.schema = schema.toLowerCase();
			this.table = table.toLowerCase();
			this.parent_table = parent_table.toLowerCase();
			this.starting_partition = starting_partition;
			this.end_partition = end_partition;
		}

	}

}


//{{#items}}
//LOAD CSV
//   FROM '{{file_name}}'
//   INTO postgresql://{{user_name}}:{{pwd}}@{{host}}}}:{{port}}/{{database}}
//        TARGET TABLE {{schema}}.{{table}}
//   WITH truncate,
//        fields terminated by '\t'
//    SET work_mem to '256 MB', maintenance_work_mem to '512 MB'
//
//BEFORE LOAD DO
//    $$ drop table {{schema}}.{{table}}; $$,
//    $$ CREATE TABLE {{schema}}.{{table}} PARTITION OF {{parent_table}}
//    FOR VALUES FROM ('{{starting_partition}}') TO ('{{end_partition}}');$$
//;
//{{/items}}

