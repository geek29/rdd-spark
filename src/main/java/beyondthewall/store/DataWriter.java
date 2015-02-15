package beyondthewall.store;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataWriter {
	
	private DataStoreConf conf = null;
	private long currentSlice = -1;
	private Map<String,SegmentWriter> writerMap = null; 
	
	public DataWriter(DataStoreConf conf){
		this.conf = conf;
		this.writerMap = new HashMap<String,SegmentWriter>();
		createDirectoryStructure();
		newSlice(System.currentTimeMillis());
	}
	
	private void createDirectoryStructure() {
		if(conf.getFileHelper().checkExistsThrow(conf.getRootDirectory())){
			for(String seg : conf.getSegments()){
				String segPath = conf.getRootDirectory() + File.separator + conf.getNamespace() + File.separator + seg;
				conf.getFileHelper().mkdirs(segPath);
			}
		}
	}

	public void newSlice(long ts){
		closeCurrentSlice();
		if (ts != -1)
			this.currentSlice = ts;
		else
			this.currentSlice = System.currentTimeMillis();		
		for(String segment : conf.getSegments()){
			SegmentWriter writer = new SegmentWriter(conf, segment, currentSlice);
			writerMap.put(segment, writer);
		}
	}
	
	public void closeCurrentSlice() {
		for(Map.Entry<String,SegmentWriter> entry : writerMap.entrySet()){
			entry.getValue().close();
		}
		writerMap.clear();
	}

	public void addJSON(String segment, String json){
		SegmentWriter writer = writerMap.get(segment);
		if(writer!=null)
			writer.addJSON(json);
		else {
			throw new StoreException("Cant find writer for segment <"+ segment+">");
		}
	}
	
	public void close(){
		closeCurrentSlice();
	}

	public long getCurrentSlice() {
		return currentSlice;
	}
	
	
	
}