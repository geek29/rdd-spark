package beyondthewall.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

public class DataWriterHadoopTest extends DataReadWriteTest {
	
	private final static boolean DEBUG = true;
	
	@Override
	public DataStoreConf createConf(int numSegments){
		Random random = new Random();		
		String namespace = "namespace" + random.nextLong();
		List<String> segments = new ArrayList<String>();
		for(int i=0;i<numSegments;i++)
			segments.add(""+i);
		
		Configuration configuration = new Configuration();		
		configuration.set("fs.defaultFS", "hdfs://localhost:54310");
		if(DEBUG)
			System.out.println("Dir : " + namespace);
		return new DataStoreConf("scratch/rddTest",namespace,segments,configuration);
	}	

}
