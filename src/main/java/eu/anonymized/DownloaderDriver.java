package eu.anonymized;

import java.sql.Driver;
import java.sql.DriverAction;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.anonymized.util.ConfigYaml;
import eu.anonymized.util.RetryException;
import eu.anonymized.util.Util;

public class DownloaderDriver implements DriverAction{

	private static final Logger LOG = LoggerFactory.getLogger(DownloaderDriver.class);

	public DownloaderDriver() {
	}

	@Override
	public void deregister() {

	}

	
	@SuppressWarnings("unchecked")
	public static void run(ConfigYaml config){


		Driver driver = new oracle.jdbc.OracleDriver();
		DriverAction da = new DownloaderDriver();  
		try {
			DriverManager.registerDriver(driver, da);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		
		config.getTables().stream().forEachOrdered(table->{
			if(table==null || table.trim().equals(""))return;

			Dump<String> d = new Dump<>(new CheckFileToDownload());
			
			Function<ParameterBean,String> f = (params)->d.apply(params);
			
			
			ParameterBean params= new ParameterBean(table, 
					config.getTable(table).get("query").toString(),
					config.getMap().get("directory").toString(), 
					config.getMap().get("connection").toString(),
					config.isSkipDumpTable(table));
			LOG.debug("Trying to download table {} and query {}",table,config.getTable(table).get("query").toString());
			Util.retry(f, params, 30, RetryException.class);	
/*
			LocalDate start = LocalDate.of(2017, 01, 01);
			LocalDate end = LocalDate.now();
			
			int endMonth = Integer.parseInt(end.format(DateTimeFormatter.ofPattern("yyyyMM")));

			for(LocalDate data=start;data.isBefore(end);data=data.plusMonths(1)){
				int currentMonth=Integer.parseInt(data.format(DateTimeFormatter.ofPattern("yyyyMM")));
				if(currentMonth==endMonth)break;
				
				String localTable=table+data.format(DateTimeFormatter.ofPattern("yyyyMM"));
				LOG.debug("Trying to download table {}",localTable);
				params= new ParameterBean(table, "query",directory, "connection string");
				Util.retry(f, params, 30, RetryException.class);	

			}
			*/
			
		});

		try {
			DriverManager.deregisterDriver(driver);
		} catch (SQLException e) {
			e.printStackTrace();
		}  

	}


}
