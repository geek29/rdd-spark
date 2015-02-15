package beyondthewall.store;

import java.io.File;

public class SegmentWriter {
	
	private String parent=null;
	private String segment=null;
	private FileWriter writer = null;
	private long currentSlice=-1;
	private DataStoreConf conf=null;
	
	public SegmentWriter(DataStoreConf conf, String segment, long ts){
		this.parent = conf.getRootDirectory() + File.separator + conf.getNamespace();		
		this.segment = segment;
		this.currentSlice = ts;	
		this.conf = conf;
		openWriter();
	}

	private void openWriter() {
		String path = parent + File.separator + segment + File.separator + currentSlice;
		checkDataDir();
		writer = new FileWriter(conf,path);
	}

	private void checkDataDir() {				
		if(!conf.getFileHelper().checkExists(parent)){
			throw new StoreException("Parent dir " + parent + " does not exist");
		}		
	}
	
	public void reopen(long ts){
		writer.close();
		currentSlice = ts;
		openWriter();
	}
	
	public void close(){
		writer.close();
	}
	
	public void addJSON(String json){
		writer.addJSON(json);
	}

}
