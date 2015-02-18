package beyondthewall.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;


public interface FileHelper extends Serializable{
	
	//Serializable is due to spark closure being sent over wire
	//Need to make it transient
	
	public boolean checkExistsThrow(String path);
	
	public boolean checkExists(String path);
	
	public boolean mkdirs(String path);
	
	public String[] children(String path);

	public OutputStream openStream(String path) throws IOException;

	public InputStream openInputStream(String path) throws IOException;

}
