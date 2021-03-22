package eu.anonymized.util;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.anonymized.App;
import eu.anonymized.Constants;
import eu.anonymized.crypto.CryptoUtils;

public class AnonymizeFile {
	private static ExecutorService ex;
	private static final Logger LOG = LoggerFactory.getLogger(AnonymizeFile.class);

	public AnonymizeFile() {
	}


	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {

		LOG.info("Starting........");
		if(args==null || args.length<1){
			System.out.println("Param 1 - Configuration file");
			LOG.debug("End with error");
			System.exit(0);
		}

		ConfigYaml.initConfig(args[0]);

		ex = Executors.newWorkStealingPool();		
		String dirOut= ConfigYaml.getInstance().getMap().get("directory").toString();

		ConfigYaml.getInstance().getFilesConfig().values().forEach(x->{
			
			try {
				Map<Object, Object> properties = (Map)x;

				AnonymizeFile d= new AnonymizeFile();
				File f = new File(properties.get("file").toString());
				d.run(f.getAbsolutePath(), "chiave",dirOut+"/"+f.getName() +"."+Constants.FILE_DOWNLOADED, (List<String>)((Map)x).get("column"));
			}catch(Exception e) {
				e.printStackTrace();
			}


		});

		try {
			ex.shutdown();
			boolean result=ex.awaitTermination(48, TimeUnit.HOURS);
			if(result) {
				System.out.println("All tasks completed.");
			}else {
				System.out.println("Application terminated for timedout.");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("END");


		


	}

	public void run(String fileIn, String password,String fileOut, List<String> column) throws Exception{
		Runnable r = () ->{
			
			boolean getheader = true;
			List<Integer> indexes=new ArrayList<>();
			
			try(BufferedReader br = new BufferedReader(new FileReader(fileIn));
					BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(fileOut))	) {
				String tmp;
				System.out.println("Starting file " + fileIn);
				for(String line; (line = br.readLine()) != null; ) {
					StringTokenizer st =  new StringTokenizer(line,"\t");
					List<String> columns=new ArrayList<String>();
					
					while (st.hasMoreTokens()) {
						String ll = st.nextToken();
						columns.add(ll);
			        }
					
					if(getheader) {
						
						indexes.addAll(Util.getColumnIndex(columns, column));
						getheader=false;
						continue;
					}
					
					StringBuffer buf = new StringBuffer();
					IntStream.range(0, columns.size()).forEachOrdered(l->{
						String field = columns.get(l);
						
						if(indexes != null && indexes.contains(l)) {
							try {
								buf.append(CryptoUtils.hash(field, "chiave")) ;
							} catch (NoSuchAlgorithmException | IOException e) {
								e.printStackTrace();
							}
						}else {
							buf.append(columns.get(l));
						}
						if(l<columns.size()-1) {
							buf.append("\t");
						}
						
					});
					
					fos.write(buf.toString().getBytes());
					fos.write("\r\n".getBytes());

				}
				System.out.println("File "+ fileIn + " done at "+LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		ex.execute(r);

	}
}
