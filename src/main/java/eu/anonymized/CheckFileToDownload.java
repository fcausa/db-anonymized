package eu.anonymized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class CheckFileToDownload implements Predicate<File> {
	
	public static Pattern DATE_PATTERN = Pattern.compile(
		      ".*\\d{4}\\d{2}."+Constants.FILE_DOWNLOADED);
	
	
	public static void main(String[] args) {
		
		String cc="sd	s";
		String cc1=cc+"\t"+"rrtr";
		System.out.println(cc);
		System.out.println(cc1);
		System.out.println(cc1.replaceAll("\t", ""));
		
		String a = "bsa201712dasd."+Constants.FILE_DOWNLOADED;
		System.out.println(DATE_PATTERN.matcher(a).matches());
		
		Pattern p = Pattern.compile("\\d{4}\\d{2}");
		Matcher m = p.matcher(a);
		String data=null;
		if(m.find()) {
			data=m.group();
		}
		LocalDate dd = LocalDate.of(Integer.parseInt(data.substring(0, 4)), Integer.parseInt(data.substring(4)), 1);
		
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");

		System.out.println(dd.format(dtf));
		
	}
	
	private static final Logger LOG = LoggerFactory.getLogger(CheckFileToDownload.class);
	
	@Override
	public boolean test(File fileToCheck) {
		if(fileToCheck.exists() && !fileToCheck.isDirectory()){
			
			if(DATE_PATTERN.matcher(fileToCheck.getName()).matches()) {
				return false;
			}
			
			BasicFileAttributes attributes = null;
			try
			{
				attributes =
						Files.readAttributes(fileToCheck.toPath(), BasicFileAttributes.class);
			}
			catch (IOException exception)
			{
				LOG.error("Exception handled when trying to get file " +
						"attributes: " + exception.getMessage(),exception);
			}

			long milliseconds = attributes.lastModifiedTime().to(TimeUnit.MILLISECONDS);
			LOG.debug("Current millisecond "+System.currentTimeMillis());
			LOG.debug("File  millisecond "+milliseconds);
//			older than 20days
			if((System.currentTimeMillis()-milliseconds)>=1728000000) {
				LOG.info("File {} already dumped but obsolete. Start a new download for {}",fileToCheck.getName(),fileToCheck.getName());
				fileToCheck.delete();
				return true;
			}else {
				return false;
			}
			
			
		}
		return true;
	}

}
