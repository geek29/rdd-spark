package beyondthewall.store;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

public class DataStoreConf {
	
	public static final int LOCAL_FILE=1;
	public static final int HADOOP_FILE=2;	
	private final String rootDirectory;
	private final String namespace;
	private final List<String> segments;
	private final FileHelper fileHelper;
	private Configuration hadoopConf;
	
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
	

}
