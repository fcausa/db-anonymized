package eu.anonymized;
import java.io.File;
import java.nio.file.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.anonymized.util.ConfigYaml;

class Executor {

	private String configurationFile;
	private static final Logger LOG = LoggerFactory.getLogger(Executor.class);



	/**
	 * @param downloadDirectory
	 * @param listTables
	 * @param user
	 * @param password
	 */
	public Executor(String configurationFile) {
		this.configurationFile=configurationFile;
	}



	
	public void download(){
		
		try {
			
			DownloaderDriver.run(ConfigYaml.initConfig(configurationFile));
		} catch (Exception e) {
			LOG.error("Cannot start download",e);
		}
	}



}    