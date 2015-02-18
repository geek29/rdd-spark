package beyondthewall.store;

import java.io.File;
import static beyondthewall.store.FileReader.EMPTY_JSON_PATH_RESULT;

public class SegmentReader implements DataIterator {
	
	private FileReader fileReader = null;
	private Collector collector = null;
	private SegmentKey key = null;
	private DataStoreConf conf = null;
	
	public SegmentReader(DataStoreConf conf,SegmentKey key, Collector collector, Query query){
		this.conf = conf;
		this.key = key;
		this.collector = collector;
		openReader(query);
	}

	private void openReader(Query query) {
		String path = conf.getRootDirectory() + File.separator + conf.getNamespace() 
				+ File.separator + key.getName() + File.separator + key.getTs();
		conf.getFileHelper().checkExistsThrow(path);		
		fileReader = new FileReader(conf, path, collector, query);
	}
	
	/*
	public void start(){	
		collector.startSegment(key.getName());
		fileReader.start();
		collector.endSegment(key.getName());
	}*/
	
	public Integer start(){
		int count=0;
		Object object = null;
		while(true){
			object = readNext();			
			if(object==null)
				break;
			if(!EMPTY_JSON_PATH_RESULT.equals(object))
				collector.collect(key, object);
			count++;
		}
		return count;
	}
	
	public Object readNext(){		
		Object obj = fileReader.readNext();
		if(obj==null){
			//fileReader.close();
			fileReader = null;
		}
		
		if(!EMPTY_JSON_PATH_RESULT.equals(obj))
			return obj;
		else return null;
	}

}
