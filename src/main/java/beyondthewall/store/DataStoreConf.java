package beyondthewall.store;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

public class DataStoreConf implements Externalizable{
	//TODO : Implements Externalizable to reduce ser-de cost
	public static final int LOCAL_FILE=1;
	public static final int HADOOP_FILE=2;	
	private String rootDirectory;
	private String namespace;
	private List<String> segments;
	
	//FileHelper needs to be made transient or handle it in different ways
	private FileHelper fileHelper;
	private Configuration hadoopConf;
	
	public DataStoreConf(){
		//No-arg constructor for de-ser
	}
	
	public DataStoreConf(String dir, String namespace, List<String> segments){
		this.rootDirectory = dir;
		this.namespace = namespace;
		this.segments = segments;		
		this.fileHelper = new LocalFileHelper();
	}
	
	public DataStoreConf(String dir, String namespace, List<String> segments, Configuration hadoopConf){
		this.rootDirectory = dir;
		this.namespace = namespace;
		this.segments = segments;
		this.hadoopConf = hadoopConf;
		this.fileHelper = new HadoopFileHelper(hadoopConf);		
	}
	
	public String getRootDirectory() {
		return rootDirectory;
	}
	
	public String getNamespace() {
		return namespace;
	}

	public List<String> getSegments() {
		return segments;
	}

	public FileHelper getFileHelper() {
		return fileHelper;
	}

	public Configuration getHadoopConf() {
		return hadoopConf;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(rootDirectory);
		out.writeObject(namespace);
		out.writeObject(segments);
		if(this.hadoopConf!=null){
			out.writeBoolean(true);			
	    	String nameNode = hadoopConf.get(HadoopFileHelper.NAME_NODE);
	    	out.writeObject(nameNode);
		}
		else out.writeBoolean(false);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		rootDirectory = (String) in.readObject();
		namespace = (String) in.readObject();
		segments = (List<String>) in.readObject();
		boolean isHDFS = in.readBoolean();
		if (isHDFS) {
			String nameNode = (String) in.readObject();
			Configuration hadoopConf = new Configuration();
			hadoopConf.set(HadoopFileHelper.NAME_NODE, nameNode);
			this.hadoopConf = hadoopConf;
			this.fileHelper = new HadoopFileHelper(hadoopConf);
		} else {
			this.fileHelper = new LocalFileHelper();
		}
	}		
	

}

