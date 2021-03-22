package eu.anonymized.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Util {
	
	private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    /**
     * Retry to run a function a few times, retry if specific exceptions occur.
     *
     * @param timeoutExceptionClasses what exceptions should lead to retry. Default: any exception
     */
    @SuppressWarnings("unchecked")
	public static <R,T> T retry(Function<R,T> function, R r, int maxRetries, Class<? extends Exception>... timeoutExceptionClasses) {
        timeoutExceptionClasses = timeoutExceptionClasses.length == 0 ? new Class[]{Exception.class} : timeoutExceptionClasses;
        int retryCounter = 0;
        Exception lastException = null;
        while (retryCounter < maxRetries) {
            try {
                return function.apply(r);
            } catch (Exception e) {
                lastException = e;
                if (Arrays.stream(timeoutExceptionClasses).noneMatch(tClass ->
                        tClass.isAssignableFrom(e.getClass())
                ))
                    throw lastException instanceof RuntimeException ?
                            ((RuntimeException) lastException) :
                            new RuntimeException(lastException);
                else {
                    retryCounter++;
                    LOG.error("FAILED - Command failed on retry " + retryCounter + " of " + maxRetries,e);
                    try {
						Thread.sleep(10000*retryCounter);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
                    if (retryCounter >= maxRetries) {
                        break;
                    }
                }
            }
        }
        throw lastException instanceof RuntimeException ?
                ((RuntimeException) lastException) :
                new RuntimeException(lastException);
    }
    
    public static List<Integer> getColumnIndex(List<String> header,List<String> column) {
    	List<Integer> result = new ArrayList<>();
    	IntStream.range(0, header.size()).forEachOrdered(idx->{
    		if(column.contains(header.get(idx))) {
    			result.add(idx);
    		}
    	});
    	return result;
    	
    }

    /** Only TEST */
    public static void main(String... args) throws Exception {
        retry((s) -> {
        	System.out.println(s);
        	if(s.length()==4)throw new ArithmeticException("boom");
            System.out.println(5 / 0);
            return null;
        }, "ciao",5, ArithmeticException.class);
    }
}