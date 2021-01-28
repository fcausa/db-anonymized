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
	private static ConfigYaml configurator = null;


	private ConfigYaml() {}

	public static ConfigYaml initConfig(String path) {

		if(configurator!=null) return configurator;
		configurator = new ConfigYaml();
		configurator.init(path);
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
			System.out.println("Config: " + config);


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
	public  Map<String,String> getTable(String tableName) {
		return (Map)((Map)config.get("entities")).get(tableName);
	}





	public static void main(String[] args) {
		ConfigYaml config = ConfigYaml.initConfig("/Users/fcausa/git/db-anonymized/src/main/resources/config.yaml");
		//c.init("/Users/fcausa/git/db-anonymized/src/main/resources/config.yaml");
		System.out.println(config.getTable("EXPORT_CALL_MEMO").get("query"));
		System.out.println(config.getTable("EXPORT_CALL_MEMO").get("description"));
		System.out.println(config.getMap().get("connection"));
		
	}
}