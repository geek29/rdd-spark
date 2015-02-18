package beyondthewall.store;

import java.io.Externalizable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HadoopFileHelper implements FileHelper, Externalizable {
	
	private transient FileSystem fs;
	private String nameNode;
	public final static String NAME_NODE = "fs.defaultFS";
	
	public HadoopFileHelper(){
		//No-arg constructor for deser
	}

	public HadoopFileHelper(Configuration hadoopConf) {
		try {
			this.fs = FileSystem.newInstance(hadoopConf);
			this.nameNode = hadoopConf.get(NAME_NODE);
		} catch (IOException e) {
			throw new StoreException(e);
		}
	}

	@Override
	public boolean checkExistsThrow(String path) {
		try {
			if (fs.exists(new Path(path)))
				return true;
			else
				throw new StoreException("Path <" + path + "> does not exist");
		} catch (IllegalArgumentException e) {
			throw new StoreException(e);
		} catch (IOException e) {
			throw new StoreException(e);
		}		
	}

	@Override
	public boolean checkExists(String path) {
		try {
			return fs.exists(new Path(path));
		} catch (IllegalArgumentException e) {
			throw new StoreException(e);
		} catch (IOException e) {
			throw new StoreException(e);
		}
	}

	@Override
	public boolean mkdirs(String path) {
		try {
			return fs.mkdirs(new Path(path));
		} catch (IllegalArgumentException e) {
			throw new StoreException(e);
		} catch (IOException e) {
			throw new StoreException(e);
		}		
	}

	@Override
	public String[] children(String path) {
		Path hp = new Path(path);
		try {
			RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(hp, false);
			List<String> list = new ArrayList<String>();			
			FileStatus[] fss = fs.listStatus(hp);
			for(FileStatus fsss : fss){
				Path p = fsss.getPath();				
				list.add(p.getName());
			}			
			return list.toArray(new String[]{});
		} catch (FileNotFoundException e) {
			throw new StoreException(e);
		} catch (IOException e) {
			throw new StoreException(e);
		}
	}

	@Override
	public OutputStream openStream(String path) throws IOException {
		return fs.create(new Path(path));		 
	}

	@Override
	public InputStream openInputStream(String path) throws IOException {
		return fs.open(new Path(path));		
	}
	
	@Override
	public void writeExternal(ObjectOutput oo) throws IOException 
	{
	    oo.writeObject(nameNode);
	}

	@Override
	public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException 
	{
	    this.nameNode = (String) oi.readObject();
	    try {
	    	Configuration conf = new Configuration();
	    	conf.set(NAME_NODE, nameNode);
			this.fs = FileSystem.newInstance(conf);			
		} catch (IOException e) {
			throw new StoreException(e);
		}
	}

}
