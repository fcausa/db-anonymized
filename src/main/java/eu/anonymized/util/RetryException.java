package eu.anonymized.util;

public class RetryException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2722720151449647111L;


	public RetryException() {
	}

	public RetryException(String message) {
		super(message);
	}

	public RetryException(Throwable cause) {
		super(cause);
	}

	public RetryException(String message, Throwable cause) {
		super(message, cause);
	}

	public RetryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
