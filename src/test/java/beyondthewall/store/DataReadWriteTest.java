package beyondthewall.store;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import junit.framework.TestCase;

public class DataReadWriteTest extends TestCase {
	
	private final static boolean DEBUG = false;
	
	public DataStoreConf createConf(int numSegments){
		Random random = new Random();
		File dir = new File("/tmp/rddTest");
		dir.mkdir();
		assertTrue(dir.exists());
		String namespace = "namespace" + random.nextLong();
		List<String> segments = new ArrayList<String>();
		for(int i=0;i<numSegments;i++)
			segments.add(""+i);
		return new DataStoreConf(dir.getAbsolutePath(),namespace,segments);
	}
	
	public void testDataWriteAddSlice(){
		
		DataStoreConf conf = createConf(5);
		DataWriter writer = new DataWriter(conf);
		
		//add 5 to default slice, closeSlice, readThrough reader		
		String objectTemplate = "{id: ~id, name : [name]}";
		
		for(int i=0;i<5;i++){
			String t = objectTemplate.replace("~id", ""+i);
			t = t.replace("[name]", "Name"+i);
			writer.addJSON(""+i, t);
			if(DEBUG)
				System.out.println("Write " + t + " in slice=" + writer.getCurrentSlice() + " seg="+ i);
		}
		long ts = writer.getCurrentSlice();
		writer.closeCurrentSlice();

		
		final ConcurrentLinkedQueue<Object> resultList = new ConcurrentLinkedQueue<Object>();
		DataReader reader = new DataReader(conf);
		reader.query(new Collector(){
			@Override
			public void collect(SegmentKey key, Object object) {
				if(DEBUG)
					System.out.println("Read " + object + " segment=" + key.getName() + " slice="+ key.getTs());
				resultList.add(object);				
			}			
		}, Query.range(Long.MIN_VALUE, Long.MAX_VALUE));
		assertEquals(5,resultList.size());
		
		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {			
		}
		
		
		//addSlice, add 10 more records, close, readThrough reader
		writer.newSlice(-1);
		for(int i=5;i<15;i++){
			String segmentNum = ""+ ((i-5)%5); 
			String t = objectTemplate.replace("~id", ""+i);
			t = t.replace("[name]", "Name"+i);
			writer.addJSON(segmentNum, t);
			if(DEBUG)
				System.out.println("Write " + t + " in slice=" + writer.getCurrentSlice() + " seg="+ segmentNum);
		}		
		writer.close();
		
		final ConcurrentLinkedQueue<Object> resultList2 = new ConcurrentLinkedQueue<Object>();
		reader = new DataReader(conf);
		
		Map<String,SortedSet<Long>> metaData = reader.getMetaData();
		if(DEBUG)
			System.out.println("MetaData : " + metaData);
		reader.query(new Collector(){
			@Override
			public void collect(SegmentKey key, Object object) {
				if(DEBUG)
					System.out.println("Read " + object + " segment=" + key.getName() + " slice="+ key.getTs());
				resultList2.add(object);				
			}			
		}, Query.range(ts+1, Long.MAX_VALUE));
		assertEquals(10,resultList2.size());
		
		//Query for all records
		final ConcurrentLinkedQueue<Object> resultList3 = new ConcurrentLinkedQueue<Object>();
		reader = new DataReader(conf);
		reader.query(new Collector(){
			@Override
			public void collect(SegmentKey key, Object object) {
				if(DEBUG)
					System.out.println("Read " + object + " segment=" + key.getName() + " slice="+ key.getTs());
				resultList3.add(object);				
			}			
		}, Query.range(Long.MIN_VALUE, Long.MAX_VALUE));
		assertEquals(15,resultList3.size());
		
	}
	
	public void testReaderRefresh(){
		DataStoreConf conf = createConf(5);
		DataWriter writer = new DataWriter(conf);
		
		//add 5 to default slice, closeSlice, readThrough reader
		
		String objectTemplate = "{id: ~id, name : [name]}";
		
		for(int i=0;i<5;i++){
			String t = objectTemplate.replace("~id", ""+i);
			t = objectTemplate.replace("[name]", "Name"+i);
			writer.addJSON(""+i, t);
			if(DEBUG)
				System.out.println("Write " + t + " in slice=" + writer.getCurrentSlice() + " seg="+ i);
		}		
		writer.closeCurrentSlice();

		
		//create reader first
		DataReader reader = new DataReader(conf);		
		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {			
		}
		
		
		//addSlice, add 20 more records, close, readThrough reader
		writer.newSlice(-1);
		for(int i=5;i<25;i++){
			String segmentNum = ""+ ((i-5)%5); 
			String t = objectTemplate.replace("~id", ""+i);
			t = t.replace("[name]", "Name"+i);
			writer.addJSON(segmentNum, t);
			if(DEBUG)
				System.out.println("Write " + t + " in slice=" + writer.getCurrentSlice() + " seg="+ segmentNum);
		}		
		writer.close();
		reader.refresh();
				
		final ConcurrentLinkedQueue<Object> resultList = new ConcurrentLinkedQueue<Object>();
		reader.query(new Collector(){
			@Override
			public void collect(SegmentKey key, Object object) {
				if(DEBUG)
					System.out.println("Read " + object + " segment=" + key.getName() + " slice="+ key.getTs());
				resultList.add(object);				
			}			
		}, Query.range(Long.MIN_VALUE, Long.MAX_VALUE));
		assertEquals(25,resultList.size());
		
	}
	
	public void testQuery(){
		int numObjects[] = {4,6,7,4,9 };
		long[] timestamps = new long[5];
		
		DataStoreConf conf = createConf(10);
		DataWriter writer = new DataWriter(conf);
		
		//add 5 to default slice, closeSlice, readThrough reader		
		String objectTemplate = "{id: ~id, name : [name]}";
		for(int j=0;j<5;j++){
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {}
			
			for(int i=0;i<numObjects[j];i++){
				String segmentNum =null;				
				segmentNum = ""+i;
				String t = objectTemplate.replace("~id", ""+i);
				t = t.replace("[name]", "Name"+i);
				if(DEBUG)
					System.out.println("For i="+i + " segmentNum="+segmentNum);
				writer.addJSON(segmentNum, t);
				if(DEBUG)
					System.out.println("Write " + t + " in slice=" + writer.getCurrentSlice() + " seg="+ segmentNum);
			}
			timestamps[j] = writer.getCurrentSlice();			
			if(j!=4)
				writer.newSlice(-1);
		}
		writer.close();
		
		long queryIntervals[][] = {
				{Long.MIN_VALUE,timestamps[0]-1},
				{timestamps[0]-1, timestamps[1]-1},
				{timestamps[1], timestamps[2]},
				{timestamps[2], Long.MAX_VALUE},				
		};
		
		long expectedSize[] = {
			0, 4, 13, 20
		};
		
		DataReader reader = new DataReader(conf);
		
		for(int i=0;i<expectedSize.length;i++){
			final ConcurrentLinkedQueue<Object> resultList = new ConcurrentLinkedQueue<Object>();
			
			reader.query(new Collector(){
				@Override
				public void collect(SegmentKey key, Object object) {
					if(DEBUG)
						System.out.println("Read " + object + " segment=" + key.getName() + " slice="+ key.getTs());
					resultList.add(object);				
				}			
			}, Query.range(queryIntervals[i][0], queryIntervals[i][1]));
			if(DEBUG)
				System.out.println("for testcase "+i + " resultSize="+ resultList.size());
			assertEquals(expectedSize[i],resultList.size());
		}
		
	}
	
	public void testQueryJSON(){
		
		DataStoreConf conf = createConf(10);
		DataWriter writer = new DataWriter(conf);		
		
		String objectTemplate = "{ \"id\": {{genId}} , \"name\" : \"{{genName}}\" }";
		
		for(int i=0;i<25;i++){
			String segmentNum = ""+ (i%5); 
			String t = objectTemplate.replace("{{genId}}", ""+i);
			t = t.replace("{{genName}}", "Name"+i);
			writer.addJSON(segmentNum, t);
			if(DEBUG)
				System.out.println("Write " + t + " in slice=" + writer.getCurrentSlice() + " seg="+ i);
		}
		long ts = writer.getCurrentSlice();
		writer.closeCurrentSlice();

		
		final ConcurrentLinkedQueue<Object> resultList = new ConcurrentLinkedQueue<Object>();
		DataReader reader = new DataReader(conf);
		reader.query(new Collector(){
			@Override
			public void collect(SegmentKey key, Object object) {
				if(DEBUG)
					System.out.println("Read " + object + " segment=" + key.getName() + " slice="+ key.getTs());
				resultList.add(object);				
			}			
		}, Query.range(Long.MIN_VALUE, Long.MAX_VALUE).jsonPath("[?(@.id < 4)]"));
		assertEquals(4,resultList.size());
		resultList.clear();
		
		reader.query(new Collector(){
			@Override
			public void collect(SegmentKey key, Object object) {
				if(DEBUG)
					System.out.println("Read " + object + " segment=" + key.getName() + " slice="+ key.getTs());
				resultList.add(object);				
			}			
		}, Query.range(Long.MIN_VALUE, Long.MAX_VALUE).jsonPath("[?(@.id < 20)]"));
		assertEquals(20,resultList.size());
		resultList.clear();
		
	}
	
	public void testForSegment(){
		DataStoreConf conf = createConf(10);
		DataWriter writer = new DataWriter(conf);		
		
		String objectTemplate = "{ \"id\": {{genId}} , \"name\" : \"{{genName}}\" }";
		
		for(int k=1;k<=5;k++)
		for(int i=0;i<k;i++){
			String segmentNum = ""+k; 
			String t = objectTemplate.replace("{{genId}}", ""+i);
			t = t.replace("{{genName}}", "Name"+i);
			writer.addJSON(segmentNum, t);
			if(DEBUG)
				System.out.println("Write " + t + " in slice=" + writer.getCurrentSlice() + " seg="+ i);
		}
		writer.close();

		for(int k=1;k<=5;k++){
			final List<Object> resultList = new ArrayList<Object>();
			DataReader reader = new DataReader(conf);
			reader.query(new Collector(){
				@Override
				public void collect(SegmentKey key, Object object) {
					if(DEBUG)
						System.out.println("Read " + object + " segment=" + key.getName() + " slice="+ key.getTs());
					resultList.add(object);				
				}			
			}, Query.range(Long.MIN_VALUE, Long.MAX_VALUE),""+k);
			//System.out.println("For "+ k + "th segment resultList="+resultList.size());
			assertEquals(k,resultList.size());
			resultList.clear();
		}
	}
	
	/*
	public void testForSegmentForRangeForJsonPath(){
		
	}*/

}
