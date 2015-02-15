package beyondthewall.store;

import java.io.IOException;
import java.io.ObjectOutputStream;

/***
 * 
 * Need to create another temp file just to indicate that file is still open - some sort of lock
 * 
 * Optionally extra index file for further timestamp based navigation
 * @author tushark
 *
 */
public class FileWriter {
	
	private String path=null;
	private ObjectOutputStream oos = null;
	private DataStoreConf conf=null;
	
	public FileWriter(DataStoreConf conf, String path){
		this.conf = conf;
		this.path = path;
		open();
	}

	public void open() {
		try {			
			oos = new ObjectOutputStream(conf.getFileHelper().openStream(path));
		} catch (IOException e) {
			throw new StoreException(e);
		}		
	}
	
	public void addJSON(String json){
		try {
			oos.writeObject(json);
		} catch (IOException e) {
			throw new StoreException(e);
		}
	}
	
	public void close(){
		try {
			oos.close();
		} catch (IOException e) {
			throw new StoreException(e);
		}
	}

}