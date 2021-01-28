package eu.anonymized;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App 
{
	
	private static final Logger LOG = LoggerFactory.getLogger(App.class);
    
	public static void main( String[] args )
    {
    	
    	LOG.info("Starting........");
        if(args==null || args.length<1){
        	System.out.println("Param 1 - Configuration file");
        	LOG.debug("End with error");
        	System.exit(0);
        }
        
        while(true){
	        new Executor(args[0]).download();
	        LOG.info("END!!!!!");
	        //15 mins
	        try {
				Thread.sleep(900000);
			} catch (InterruptedException e) {
					LOG.error("Thread sleep exception",e);
			}
	        LOG.info("Let's start another round!!!");
        }
        
    }
}
