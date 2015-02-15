package beyondthewall.store;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class DataReader {
	
	private static final int NUM_THREADS = 4;
	private Map<String, SortedSet<Long>> fileStructure=null;
	private ExecutorService executor = null;
	private CompletionService completionService = null;
	private DataStoreConf conf = null;
	
	public DataReader(DataStoreConf conf){
		this.conf = conf;
		this.executor = Executors.newFixedThreadPool(NUM_THREADS);
		completionService = new ExecutorCompletionService(executor);
		fileStructure = readMetadata();
	}

	private Map<String, SortedSet<Long>> readMetadata() {
		Map<String, SortedSet<Long>> fileStructure = new HashMap<String,SortedSet<Long>>();		
		String path = conf.getRootDirectory() + File.separator + conf.getNamespace();
		conf.getFileHelper().checkExistsThrow(path);
		String[] segments = conf.getFileHelper().children(path);
		for(String seg : segments){
			String segPath = path + File.separator + seg; 
			String[] slices = conf.getFileHelper().children(segPath);
			SortedSet<Long> set = new TreeSet<Long>();
			for(String slice : slices){
				set.add(Long.valueOf(slice));
			}
			fileStructure.put(seg,set);
		}
		return fileStructure;
	}
	
	public void refresh(){
		fileStructure = readMetadata();
	}
	
	public Map<String, SortedSet<Long>> getMetaData() {
		return fileStructure;
	}

	public List<SegmentKey> findRange(long from, long to) {
		List<SegmentKey> list = new ArrayList<SegmentKey>();
		for(Map.Entry<String,SortedSet<Long>> e : fileStructure.entrySet()){
			SortedSet<Long> slices = e.getValue();
			String seg = e.getKey();
			SortedSet<Long> range = slices.tailSet(from);
			for(Long l : range){
				if(l<=to){
					SegmentKey key = new SegmentKey(seg,l);
					list.add(key);
				}
			}
		}
		Collections.sort(list);
		return list;
	}
	
	
	@SuppressWarnings("unchecked")
	public void query(Collector collector, Query query, String segment){		
		long from = query.getFrom();
		long to = query.getTo();				
		List<SegmentKey> targetedSegments = findRange(from,to);
		for(SegmentKey key : targetedSegments){
			if(segment.equals(key.getName())){				
				final SegmentReader reader = new SegmentReader(conf, key, collector, query);				
				reader.start();
				break;
			}
		}
	}

	@SuppressWarnings("unchecked")
	public void query(Collector collector, Query query){		
		long from = query.getFrom();
		long to = query.getTo();
		String jsonPath= query.getJsonPath();
		Set<String> segmentFilter = query.getSegmentFilter();
		
		List<SegmentKey> targetedSegments = findRange(from,to);
		for(SegmentKey key : targetedSegments){
			if(segmentFilter==null || segmentFilter.contains(key.getName())){				
				final SegmentReader reader = new SegmentReader(conf, key, collector, query);			
				completionService.submit(new Callable<Integer>(){
					public Integer call(){
						return reader.start();
					}
				});
				
			}
		}
		
		int totalFuturesSubmitted = segmentFilter==null ? targetedSegments.size() : segmentFilter.size();
		int totalObjs=0;
		for(int i=0;i<totalFuturesSubmitted;i++){
			Future<Integer> resultFuture;			
			try {
				resultFuture = completionService.take();
				totalObjs += resultFuture.get();
			} catch (InterruptedException e) {
				throw new StoreException(e);
			} catch (ExecutionException e) {
				throw new StoreException(e);
			}			
		}
		
		/*System.out.println("Wait complete for " + totalFuturesSubmitted + " futures size=" + targetedSegments.size()
				+ " totalObjs="+ totalObjs);*/
	}

	public void close() {
		if(executor!=null){
			this.executor.shutdown();
		}		
	}


}
