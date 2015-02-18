package beyondthewall.myrdd;

import java.io.File
import java.util.ArrayList
import java.util.Random
import java.util.concurrent.ConcurrentLinkedQueue
import scala.runtime.ScalaRunTime._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import beyondthewall.store.DataReader
import beyondthewall.store.DataStoreConf
import beyondthewall.store.DataWriter
import beyondthewall.store.Query
import beyondthewall.store.TestResultCollector
import org.apache.hadoop.conf.Configuration

class DataStoreRDDSpec extends FlatSpec with ShouldMatchers {
  
	val conf = new SparkConf().setAppName("RDDSpce").setMaster("local")
	val sc = new SparkContext(conf)
  
   def createConf(numSegments: Int) : DataStoreConf = {
		val random : Random  = new Random()
		val dir : File = new File("/tmp/rddTest")
		dir.mkdir()
		dir should be ('exists)
		val namespace : String = "namespace" + random.nextLong()
		val segments : ArrayList[String] = new ArrayList[String]()
		var i = 0
		for(i <- 0 to numSegments-1){
			segments.add(String.valueOf(i))	
   		}
		new DataStoreConf(dir.getAbsolutePath(),namespace,segments)
   }
	
	
	def createHDFSConf(numSegments: Int) : DataStoreConf = {
		val random : Random  = new Random()
		val dir : File = new File("/tmp/rddTest")
		dir.mkdir()
		dir should be ('exists)
		val namespace : String = "namespace" + random.nextLong()
		val segments : ArrayList[String] = new ArrayList[String]()
		var i = 0
		for(i <- 0 to numSegments-1){
			segments.add(String.valueOf(i));			
   		}
		
		val configuration :Configuration  = new Configuration()	
		configuration.set("fs.defaultFS", "hdfs://localhost:54310")
		System.out.println("Dir : " + namespace)
		return new DataStoreConf("scratch/rddTest",namespace,segments,configuration)		
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
	   val dataStoreConf = createConf(5)
	   writeData(dataStoreConf,25)
	   checkDataWritten(dataStoreConf,25)
	   var query = Query.range(java.lang.Long.MIN_VALUE, java.lang.Long.MAX_VALUE);
	   val myrdd = sc.myrdd(dataStoreConf, query)	    
	   val collectedRDD: Array[Any] = myrdd.collect()
	   println(stringOf(collectedRDD))
	   println(collectedRDD.length)
	   collectedRDD.length should equal(25)
  }
   
   "myrdd" should "return the written data correctly using JSONPath (localconf)" in {
	   val dataStoreConf = createConf(5)
	   writeData(dataStoreConf,25)
	   checkDataWritten(dataStoreConf,25)
	   var query = Query.range(java.lang.Long.MIN_VALUE, java.lang.Long.MAX_VALUE).jsonPath("[?(@.id < 4)]");
	   val myrdd = sc.myrdd(dataStoreConf, query)	    
	   val collectedRDD: Array[Any] = myrdd.collect()
	   println(stringOf(collectedRDD))
	   println(collectedRDD.length)
	   collectedRDD.length should equal(4)     
   }
   
   "myrdd" should "return the written data correctly for given range (localconf)" in {     
	   val dataStoreConf = createConf(5)
	   val numObjects = Array(4,6,7,4,9)
	   val timestamps = Array(0L,0L,0L,0L,0L)
	   val objectTemplate : String = "{ \"id\": {{genId}} , \"name\" : \"{{genName}}\" }"
	   var i = 0
	   var j = 0
	   val writer : DataWriter = new DataWriter(dataStoreConf)
	   for(j <- 0 to 4){
	     try {
				Thread.sleep(200)
			} catch {
			  case ioe: InterruptedException => System.out.println() 
			}
			
			for(i <- 0 to numObjects(j)-1){
				val segmentNum: String  = ""+ (i%5) 
				var t : String  = objectTemplate.replace("{{genId}}", ""+i)
				t = t.replace("{{genName}}", "Name"+i)
				writer.addJSON(segmentNum, t)
				System.out.println("Write " + t + " in slice=" + writer.getCurrentSlice() + " seg="+ i)
			}
			timestamps(j) = writer.getCurrentSlice()			
			if(j!=4)
				writer.newSlice(-1);			
	   }
	   
		writer.close();
		var queryIntervals = Array.ofDim[Long](4,2) 
		queryIntervals(0)(0) = java.lang.Long.MIN_VALUE
		queryIntervals(0)(1) = timestamps(0)-1
		
		queryIntervals(1)(0) = timestamps(0)-1
		queryIntervals(1)(1) = timestamps(1)-1
		
		queryIntervals(2)(0) = timestamps(1)
		queryIntervals(2)(1) = timestamps(2)
		
		queryIntervals(3)(0) = timestamps(2)
		queryIntervals(3)(1) = java.lang.Long.MAX_VALUE	
		
		val expectedSize = Array(0, 4, 13, 20)
		
		val reader:DataReader  = new DataReader(dataStoreConf)		
		for(i <- 0 to expectedSize.length -1 ){
			val resultList : ConcurrentLinkedQueue[Object] = new ConcurrentLinkedQueue[Object]()			
			reader.query(new TestResultCollector(resultList, true), Query.range(queryIntervals(i)(0), queryIntervals(i)(1)))			
			System.out.println("for testcase "+i + " resultSize="+ resultList.size())
			resultList.size() should equal(expectedSize(i))
		}     
   }
      
   "myrdd" should "return the written data correctly using JSONPath (HDFS)" in {
	   val dataStoreConf = createHDFSConf(5)
	   writeData(dataStoreConf,25)
	   checkDataWritten(dataStoreConf,25)
	   var query = Query.range(java.lang.Long.MIN_VALUE, java.lang.Long.MAX_VALUE).jsonPath("[?(@.id < 4)]");
	   val myrdd = sc.myrdd(dataStoreConf, query)	    
	   val collectedRDD: Array[Any] = myrdd.collect()
	   println(stringOf(collectedRDD))
	   println(collectedRDD.length)
	   collectedRDD.length should equal(4)     
   }
   
   "myrdd" should "return the written data correctly for given range (HDFS)" in {   
     //TODO Test with real spark cluster
	   val dataStoreConf = createHDFSConf(5)
	   val numObjects = Array(4,6,7,4,9)
	   val timestamps = Array(0L,0L,0L,0L,0L)
	   val objectTemplate : String = "{ \"id\": {{genId}} , \"name\" : \"{{genName}}\" }"
	   var i = 0
	   var j = 0
	   val writer : DataWriter = new DataWriter(dataStoreConf)
	   for(j <- 0 to 4){
	     try {
				Thread.sleep(200)
			} catch {
			  case ioe: InterruptedException => System.out.println() 
			}
			
			for(i <- 0 to numObjects(j)-1){
				val segmentNum: String  = ""+ (i%5) 
				var t : String  = objectTemplate.replace("{{genId}}", ""+i)
				t = t.replace("{{genName}}", "Name"+i)
				writer.addJSON(segmentNum, t)
				System.out.println("Write " + t + " in slice=" + writer.getCurrentSlice() + " seg="+ i)
			}
			timestamps(j) = writer.getCurrentSlice()			
			if(j!=4)
				writer.newSlice(-1);			
	   }
	   
		writer.close();
		var queryIntervals = Array.ofDim[Long](4,2) 
		queryIntervals(0)(0) = java.lang.Long.MIN_VALUE
		queryIntervals(0)(1) = timestamps(0)-1
		
		queryIntervals(1)(0) = timestamps(0)-1
		queryIntervals(1)(1) = timestamps(1)-1
		
		queryIntervals(2)(0) = timestamps(1)
		queryIntervals(2)(1) = timestamps(2)
		
		queryIntervals(3)(0) = timestamps(2)
		queryIntervals(3)(1) = java.lang.Long.MAX_VALUE	
		
		val expectedSize = Array(0, 4, 13, 20)
		
		val reader:DataReader  = new DataReader(dataStoreConf)		
		for(i <- 0 to expectedSize.length -1 ){
			val resultList : ConcurrentLinkedQueue[Object] = new ConcurrentLinkedQueue[Object]()			
			reader.query(new TestResultCollector(resultList, true), Query.range(queryIntervals(i)(0), queryIntervals(i)(1)))			
			System.out.println("for testcase "+i + " resultSize="+ resultList.size())
			resultList.size() should equal(expectedSize(i))
		}     
   }

}