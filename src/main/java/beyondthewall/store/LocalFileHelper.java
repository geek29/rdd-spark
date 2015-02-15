package beyondthewall.store;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class LocalFileHelper implements FileHelper{
	
	public boolean checkExistsThrow(String path){
		File file = new File(path);
		if(!file.exists()){
			throw new StoreException("Parent dir " + file + " does not exist");
		} else return true;
	}
	
	public boolean checkExists(String path){
		File file = new File(path);
		if(!file.exists()){
			return false;
		} else return true;
	}
	
	public boolean mkdirs(String path){
		File file = new File(path);
		boolean result = file.mkdirs();
		return result;
	}
	
	public String[] children(String path){
		File file = new File(path);
		if(file.isDirectory()){
			String[] files = file.list();
			return files;
		} else throw new StoreException("Not a directory - " + path);
	}

	@Override
	public OutputStream openStream(String path) throws IOException {
		return new BufferedOutputStream(new FileOutputStream(new File(path)));
	}

	@Override
	public InputStream openInputStream(String path) throws IOException {
		return new BufferedInputStream(new FileInputStream(new File(path)));
	}	

}
