package beyondthewall

import org.apache.spark.SparkContext
import beyondthewall.store.Query
import beyondthewall.store.DataStoreConf
import beyondthewall.store.DataReader
import org.apache.spark.rdd.DataStoreRDD
import org.apache.spark.rdd.RDD

package object myrdd {

  implicit class DataStoreRDDSparkContext(context: SparkContext) {
    
    def myrdd(conf : DataStoreConf, query : Query): DataStoreRDD/*RDD[(Any)]*/ = {
      val myrdd = new DataStoreRDD(context, conf,query)
      myrdd
    }
    
  }

}