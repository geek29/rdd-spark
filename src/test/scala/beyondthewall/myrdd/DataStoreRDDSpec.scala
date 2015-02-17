package beyondthewall.myrdd;

import org.scalatest.FlatSpec
import scala.collection.mutable.Stack
import org.scalatest.matchers.ShouldMatchers
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.runtime.ScalaRunTime._
import java.io.File
import beyondthewall.store.DataStoreConf
import java.util.Random
import java.util.ArrayList
import beyondthewall.store.DataWriter
import java.util.concurrent.ConcurrentLinkedQueue
import beyondthewall.store.DataReader
import beyondthewall.store.SegmentKey
import beyondthewall.store.Collector
import beyondthewall.store.Query
import beyondthewall.store.TestResultCollector

class DataStoreRDDSpec extends FlatSpec with ShouldMatchers {
  
  
   def createConf(numSegments: Int) : DataStoreConf = {
		val random : Random  = new Random();
		val dir : File = new File("/tmp/rddTest");
		dir.mkdir();
		dir should be ('exists)
		val namespace : String = "namespace" + random.nextLong();
		val segments : ArrayList[String] = new ArrayList[String]();
		var i = 0
		for(i <- 0 to numSegments-1){
			segments.add(String.valueOf(i));			
   		}
		new DataStoreConf(dir.getAbsolutePath(),namespace,segments);
   }
   
   def writeData(conf : DataStoreConf, count : Integer){
	   val writer : DataWriter = new DataWriter(conf);
	   val objectTemplate : String = "{ \"id\": {{genId}} , \"name\" : \"{{genName}}\" }";
	   var i = 0;
	   for(i <- 0 to count-1){
			val segmentNum: String  = ""+ (i%5); 
			var t : String  = objectTemplate.replace("{{genId}}", ""+i);
			t = t.replace("{{genName}}", "Name"+i);
			writer.addJSON(segmentNum, t);			
			System.out.println("Write " + t + " in slice=" + writer.getCurrentSlice() + " seg="+ i);
		}
		val ts:Long  = writer.getCurrentSlice();
		writer.close();
   }
   
   def checkDataWritten(conf : DataStoreConf, count : Integer) = {
	   val resultList : ConcurrentLinkedQueue[Any]  = new ConcurrentLinkedQueue();
   	   val reader : DataReader = new DataReader(conf);
   	   var collector = new TestResultCollector(resultList,true);
	   reader.query(collector, Query.range(java.lang.Long.MIN_VALUE, java.lang.Long.MAX_VALUE).jsonPath("[?(@.id < 4)]"));
	   resultList.size() should equal(4)
	   resultList.clear();
   }

   "myrdd" should "return the written data correctly (localconf)" in {
	   val conf = new SparkConf().setAppName("RDDSpce").setMaster("local")
	   val sc = new SparkContext(conf)
	   val dataStoreConf = createConf(5)
	   writeData(dataStoreConf,25)
	   var query = Query.range(java.lang.Long.MIN_VALUE, java.lang.Long.MAX_VALUE).jsonPath("[?(@.id < 4)]");
	   val myrdd = sc.myrdd(dataStoreConf, query)	    
	   val collectedRDD: Array[Any] = myrdd.collect()
	   println(stringOf(collectedRDD))
	   println(collectedRDD.length)
	   collectedRDD.length should equal(4)
  }
   
   "myrdd" should "return the written data correctly using JSONPath (localconf)" in {
     //TODO
     
   }
   
   "myrdd" should "return the written data correctly for given range (localconf)" in {
     //TODO
     
     
   }
      
   "myrdd" should "return the written data correctly using JSONPath (HDFS)" in {
     //TODO
     
     
   }
   
   "myrdd" should "return the written data correctly for given range (HDFS)" in {
     //TODO
     
     
   }

}