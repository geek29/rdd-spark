package org.apache.spark.rdd
import org.apache.hadoop.conf.Configuration
import org.apache.spark.InterruptibleIterator
import org.apache.spark.Logging
import org.apache.spark.Partition
import org.apache.spark.SerializableWritable
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.NextIterator
import beyondthewall.store.DataReader
import beyondthewall.store.Query
import beyondthewall.store.DataStoreConf



private[spark] class StorePartition(rddId: Int, idx: Int, _segment : String, _conf: DataStoreConf, _query:Query)
  extends Partition {

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx
  
  def conf : DataStoreConf = _conf
  
  def query = _query
  
  def segment = _segment

}

class DataStoreRDD ( 
    sc : SparkContext,  conf : DataStoreConf, query : Query)
  extends RDD[Any](sc, Nil) with Logging {
  
  override def compute(split: Partition, context: TaskContext) = {  
    var storePartition = split.asInstanceOf[StorePartition]
    var dataReader = new DataReader(storePartition.conf)
    var dataIterator = dataReader.iteratorForQuery(storePartition.query, storePartition.segment)
    val iter = new NextIterator[(Any)] {      
      override def getNext() = {                        
        val json  = dataIterator.readNext()
        if(json==null)
          finished = true
        json
      }

      override def close() {
        dataReader.close()
        logInfo("Closing the iterator")
      }
    }
    logInfo("Returning compute for partition " + split.index)
    new InterruptibleIterator[Any](context, iter)   
  }

  
  protected def getPartitions: Array[Partition] = {
    val dataReader = new DataReader(conf)
    var metaData = dataReader.getMetaData().keySet().iterator()
    val array = new Array[Partition](5)
    var i:Integer = 0;
    while(metaData.hasNext()){
      var segment = metaData.next()
      array(i) = new StorePartition(id, i, segment, conf,query)
      i = i + 1
    }
    array
  }  
}