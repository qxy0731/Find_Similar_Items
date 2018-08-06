package comp9313.ass4
import scala.math._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SetSimJoin {
    def main(args: Array[String]) {
      val inputFile = args(0)
      val outputFolder = args(1)
      val threshold = args(2).toDouble
      val conf = new SparkConf().setAppName("SetSimJoin")
      val sc = new SparkContext(conf)
      //reed inputFile to RDD
      val input = sc.textFile(inputFile)
      input.persist()
      //using map-reduce to get the token popularityMap {RID => frequency}
      val popularityMap = input.flatMap(line => line.split(" ").tail)
      .map(x=>(x,1)).reduceByKey(_+_) //map-reduce to get frequency
      .sortBy(x => x._2).collectAsMap //collect to map
      //broadcast popularity
      val global_popularity = sc.broadcast(popularityMap)


      // to get (rid,content,prefix) tuple
      val rid_content_prefix = input.map(line => line.split(" "))
      .map(x => (x.head.toInt,x.tail.sortBy(x => (global_popularity.value(x), x)))) //sort content with popularity
      .map(x => (x._1,x._2,x._2.take(x._2.length- math.ceil(x._2.length * threshold).toInt + 1))) //extra prefix


      // to get (each prefix token, (rid,content))
      val prefixToken_ridContenPair = rid_content_prefix.flatMap(
          x => x._3.map(prefixToken => (prefixToken.toInt, (x._1,x._2)))
          //for each token in prefix, connect it with corresponding (rid,content) pair
      ).groupByKey


      //first filtered by length
      //then get result pairs by calculate similarity of candidate pairs
      def my_filter(member:(Int, Iterable[(Int, Array[String])])) : ArrayBuffer[((Int,Int), Double)] = {
        val result = ArrayBuffer[((Int,Int),Double)]()
        val candidates = member._2.toArray
        for (i <- 0 to candidates.length -1){
          for(j<- i+1 to candidates.length -1){
            val candidate1 = candidates(i)._2.toSet
            val candidate2 = candidates(j)._2.toSet
            if (math.ceil(candidate1.size * threshold) <= candidate2.size){ //length filter
              val intersection_size = (candidate1 intersect candidate2).size.toDouble
              //calculate similarity
              val similarity = intersection_size / (candidate1.size + candidate2.size - intersection_size).toDouble
              //candidate pair
              val pair = (candidates(i)._1,candidates(j)._1)
              //if similarity is more than threshold, add to result
              if(similarity >= threshold)  result.append((pair,similarity))
            }
          }
        }
        //return the result ((rid1,rid2),similarity)
        result
      }


      //get the final output
      val pairID_similarity = prefixToken_ridContenPair.flatMap ( x => my_filter(x)) // get (rid1,rid2),similarity) by my_filter
      .reduceByKey((a,b)=>a) //get distinct pairs
      .sortBy(x=>(x._1._1,x._1._2)).map(x => x._1 + "\t" + x._2) //sort by the first rid and then the second


      //from RDD to disk
      pairID_similarity.saveAsTextFile(outputFolder)
  }
}
