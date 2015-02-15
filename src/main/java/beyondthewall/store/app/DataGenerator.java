package beyondthewall.store.app;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.conf.Configuration;

import beyondthewall.store.Collector;
import beyondthewall.store.DataReader;
import beyondthewall.store.DataStoreConf;
import beyondthewall.store.DataWriter;
import beyondthewall.store.Query;
import beyondthewall.store.SegmentKey;
import beyondthewall.store.StoreException;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

/**
 * 
 * This program reads json file generated from http://www.json-generator.com/ and stores it inside S
 * @author tushark
 *
 */
public class DataGenerator {
		
    public static List<User> readJsonStream(InputStream in) throws IOException {
    	Gson gson = new Gson();
        JsonReader reader = new JsonReader(new InputStreamReader(in, "UTF-8"));
        List<User> users = new ArrayList<User>();
        reader.beginArray();
        while (reader.hasNext()) {
        	User user = gson.fromJson(reader, User.class);
        	users.add(user);
        }
        reader.endArray();
        reader.close();
        return users;
    }
    
    public static DataStoreConf createConf(int numSegments, String nameNode, String dir){
		Random random = new Random();
		String namespace = "namespace" + random.nextLong();
		List<String> segments = new ArrayList<String>();
		for(int i=0;i<numSegments;i++)
			segments.add(""+i);
		Configuration configuration = new Configuration();		
		configuration.set("fs.defaultFS", nameNode);
		System.out.println("Output directory : " + namespace);
		return new DataStoreConf(dir,namespace,segments,configuration);
	}

	public static void main(String[] args) throws IOException {
		File file = new File(args[1]);
		BufferedInputStream stream = new BufferedInputStream(new FileInputStream(file));
		List<User> users = readJsonStream(stream);
		System.out.println("Total Users " + users.size());
		Gson gson = new Gson();
		System.out.println("First User : " + gson.toJson(users.get(0)));
		
		DataStoreConf conf = createConf(5, args[0], args[2]);
		DataWriter writer =  new DataWriter(conf);
		for(User user : users){
			String id = user.get_id();
			String segmentNum = getSegment(id);
			String json = gson.toJson(user);
			writer.addJSON(segmentNum, json);
		}
		System.out.println("Wrote " + users.size() + " users..");
		writer.close();
		
		final ConcurrentLinkedQueue<Object> resultList = new ConcurrentLinkedQueue<Object>();
		DataReader reader = new DataReader(conf);
		reader.query(new Collector(){
			@Override
			public void collect(SegmentKey key, Object object) {				
				resultList.add(object);				
			}			
		}, Query.range(Long.MIN_VALUE, Long.MAX_VALUE));
		
		System.out.println("Read " + resultList.size() + " users..");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Enter query in JsonPath");
		while(true){
			resultList.clear();			
			System.out.print(">");
			String line = br.readLine();
			if("exit".equals(line))
				break;
			else {
				doQuery(reader, line, resultList);
			}			
		}		
		reader.close();		
	}

	private static void doQuery(DataReader reader , String line, final ConcurrentLinkedQueue<Object> resultList ) {
		try {
			reader.query(new Collector() {
				@Override
				public void collect(SegmentKey key, Object object) {
					resultList.add(object);
				}
			}, Query.range(Long.MIN_VALUE, Long.MAX_VALUE).jsonPath(line));
			printResult(resultList);
		} catch (StoreException e) {
			System.out.println("Error while query " + e.getMessage());
		}	
	}

	private static void printResult(ConcurrentLinkedQueue<Object> resultList) {
		StringBuilder sb = new StringBuilder();
		int count=1;
		for(Object obj : resultList){
			sb.append(count++).append(" : ").append(obj.toString()).append("\n");
		}
		System.out.println(sb.toString());
	}

	private static String getSegment(String id) {
		int code = id.hashCode();
		if(code<0)
			code *= (-1);
		return ""+code%4;
	}

}
