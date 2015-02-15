package beyondthewall.store;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.jayway.jsonpath.InvalidPathException;

public class StoreException extends RuntimeException {
	
	public StoreException(IOException ioe){
		super(ioe);
	}

	public StoreException(String string) {
		super(string);
	}

	public StoreException(ClassNotFoundException e) {
		super(e);
	}

	public StoreException(ExecutionException e) {
		super(e);
	}

	public StoreException(InvalidPathException e) {
		super(e);
	}

	public StoreException(IllegalArgumentException e) {
		super(e);
	}

	public StoreException(InterruptedException e) {
		super(e);
	}

}
