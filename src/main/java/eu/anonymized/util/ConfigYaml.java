package eu.anonymized.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class ConfigYaml {



	private static Map<String, Object> config=null;
	private static ConfigYaml configurator = new ConfigYaml();;


	private ConfigYaml() {}

	public static ConfigYaml initConfig(String path) {


		synchronized(configurator) {
			config=null;
			configurator = new ConfigYaml();
			configurator.init(path);

		}


		return configurator;
	}

	public static ConfigYaml getInstance() {
		return configurator;
	}


	public  Map<String, Object> getMap(){
		return config;
	}

	private  void init(String path) {
		InputStream inputStream;
		try {
			Yaml yaml = new Yaml();
			inputStream = new FileInputStream(path);
			config = yaml.load(inputStream);



		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//				 .getClassLoader()
		//				 .getResourceAsStream(path);

	}


	@SuppressWarnings({ "unchecked", "rawtypes" })
	public  List<String> getTables(){
		List<String> l = new ArrayList<>();
		l.addAll(((Map)config.get("entities")).keySet());
		return l;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public  Map<String,Object> getTable(String tableName) {
		return (Map)((Map)config.get("entities")).get(tableName);
	}

	public boolean isSkipDumpTable(String table) {
		if(!this.getTable(table).containsKey("skip")) return false;

		if((Boolean)this.getTable(table).get("skip")) return true;

		return false;
	}
	
	public Map getFilesConfig() {
		return ((Map)this.getMap().get("files"));
	}

	public Map getFileSection(String section ) {
		return (Map)this.getFilesConfig().get(section);
	}
	
	




	public static void main(String[] args) {
		ConfigYaml config = ConfigYaml.initConfig("/Users/fcausa/git/db-anonymized/src/main/resources/config.yaml");
		//c.init("/Users/fcausa/git/db-anonymized/src/main/resources/config.yaml");
		System.out.println(config.getTable("EXPORT_TAB_MEMO").get("query"));
		System.out.println(config.getTable("EXPORT_TAB_MEMO").get("description"));
		System.out.println(config.isSkipDumpTable("EXPORT_TAB_MEMO"));
		System.out.println(config.isSkipDumpTable("PRODUCT"));

		System.out.println(config.getMap().get("connection"));
		
		//System.out.println(config.getFilesConfig().values());
		
		config.getFilesConfig().values().forEach(x->{
			System.out.println(((Map)x).get("column"));
		});

	}
}