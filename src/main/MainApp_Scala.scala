import java.io.{FileOutputStream, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object MainApp_Scala {
  def main(args: Array[String]): Unit = {

    implicit object ListOrdering extends Ordering[List[String]]{
       override def compare(x: List[String], y: List[String]): Int = {
         var index = 0
         var m = x(index)
         var n = y(index)
         while ((m==n)&& (index < x.length)){
           index += 1
           m = x(index)
           n = y(index)
         }
         if (index >= x.length){
           0
         }else{
           m.compareTo(n)
         }
       }
     }

    Logger.getLogger("org").setLevel(Level.INFO)
    val conf = new SparkConf()
      .setAppName("task1")
      .setMaster("local[*]")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "4g")
    val sc = new SparkContext(conf)
    val start_time = System.currentTimeMillis()
    val rawData = sc.textFile(args(2)).filter(!_.equals("user_id,business_id"))
    //parameters
    val support = args(1).toInt
    val filterSupport = args(0).toInt

    val KVPairs = rawData.map(_.split(",")).map(x => (x(0), Set(x(1))))
    val basketModule = KVPairs.reduceByKey(_ | _).filter(_._2.size > filterSupport)
      .repartition(numPartitions = support / 10).values.persist()

    val candidates = basketModule.mapPartitions(SONFirstMap(10)).reduceByKey(_ | _).collect().toList
    val FrequentItemSets = basketModule.mapPartitions(SONSecondMap(candidates))
      .reduceByKey(_ + _).filter(_._2 >= support).keys
      .aggregateByKey(List.empty[Set[String]])((x, y) => y +: x, (x, y) => x ::: y).collect().toList
    val forOutputCd = mutable.Map.empty[Int, List[List[String]]]
    var largestSizeCd = 0
    for (kv <- candidates){
      forOutputCd += (kv._1 -> kv._2.map(x => x.toList.sortWith(_ < _)).toList.sorted)
      if(kv._1 > largestSizeCd)
        largestSizeCd = kv._1
    }
    val forOutputFI = mutable.Map.empty[Int, List[List[String]]]
    var largestSizeFI = 0
    for (kv <- FrequentItemSets){
      forOutputFI += (kv._1 -> kv._2.map(x => x.toList.sortWith(_ < _)).sorted)
      if(kv._1 > largestSizeFI)
        largestSizeFI = kv._1
    }
    val out = new PrintWriter(new FileOutputStream(args(3)))
    out.println("Candidates:")
    for (i <- 1 to largestSizeCd){
      out.println(forOutputCd(i).map(_.mkString("('", "', '", "')")).mkString("",",",""))
    }
    out.println("")
    out.println("Frequent Itemsets:")
    for (i <- 1 to largestSizeFI){
      out.println(forOutputFI(i).map(_.mkString("('", "', '", "')")).mkString("",",",""))
    }
    out.close()
    printf("\nDuration: %d ms", System.currentTimeMillis()-start_time)
  }
  def PCYHash(m:Set[String]): Int ={
    m.hashCode % 1e6.toInt
  }

  def SONFirstMap(chunkThreshold: Int)(chunkIter: Iterator[Set[String]]): Iterator[(Int, Set[Set[String]])] = {
    val storedChunk = chunkIter.toList
    val nameCountMap = mutable.Map.empty[Set[String], Int]
    val PCYFilter = mutable.Map.empty[Int, Int]
    for (basket <- storedChunk) {
      for (singleton <- basket){
        val key = Set(singleton)
        if(nameCountMap.contains(key)){
          nameCountMap(key) += 1
        }else{
          nameCountMap += (key -> 1)
        }
      }
      for (p <- basket.subsets(2)){
        val key = PCYHash(p)
        if(PCYFilter.contains(key)){
          PCYFilter(key) += 1
        }else{
          PCYFilter += (key -> 1)
        }
      }
    }
    val result1  = for {singleton <- nameCountMap.keys
         if nameCountMap(singleton) >= chunkThreshold} yield singleton
    var results = List((1, result1.toSet))
    //count pairs
    val frequentPairCandidates = mutable.Map.empty[Set[String], Int]
    val filterSets:Set[Set[String]] = result1.toSet
    for (p <- filterSets.subsets(2)){
      val candidate = p.flatten
      val key = PCYHash(candidate)
      if (PCYFilter.contains(key) && PCYFilter(key) >= chunkThreshold){
        frequentPairCandidates += (candidate -> 0)
      }
    }
    for (basketSet <- storedChunk){
      for (p <- frequentPairCandidates.keys){
        if (p subsetOf basketSet){
          frequentPairCandidates(p) += 1
        }
      }
    }
    val result2 = for {pair <- frequentPairCandidates.keys
                      if frequentPairCandidates(pair) >= chunkThreshold}yield pair
    results = (2, result2.toSet) +: results
    // count others
    var setSize = 3
    while (setSize < results.head._2.size){
      val candidate = candidateConstruct(chunkThreshold = chunkThreshold,
        setSize = setSize, chunk = storedChunk, filterSets = results.head._2.toList)
      results = (setSize, candidate) +: results
      setSize += 1
    }
    results.iterator
  }

  // when setSize greater than 2
  def candidateConstruct(chunkThreshold: Int,
                         setSize: Int,
                         chunk:List[Set[String]],
                         filterSets:List[Set[String]]):Set[Set[String]]={
    val candidates = mutable.Map.empty[Set[String], Int]
    for (i <- filterSets.indices; j <- i+1 until filterSets.length){
      val l = filterSets(i).toList.sorted.take(setSize-2)
      val r = filterSets(j).toList.sorted.take(setSize-2)
      if (l == r){
        candidates += ((filterSets(i)| filterSets(j)) -> 0)
      }
    }
    for (basket <- chunk){
      for (oneSet <- candidates.keys){
        if (oneSet subsetOf basket){
          candidates(oneSet) += 1
        }
      }
    }
    val result = for {oneSet <- candidates.keys
                      if candidates(oneSet) >= chunkThreshold} yield oneSet
    result.toSet
  }

  def SONSecondMap(allCandidates: List[(Int, Set[Set[String]])])(chunkIter: Iterator[Set[String]]) : Iterator[((Int, Set[String]), Int)]={
    val frequentItemSets = mutable.Map.empty[Set[String], Int]
    for (basket <- chunkIter){
      for (oneSizeSets <- allCandidates){
        for (oneSizeSet <- oneSizeSets._2){
          if (oneSizeSet subsetOf basket){
            if(frequentItemSets.contains(oneSizeSet)){
              frequentItemSets(oneSizeSet) += 1
            }else{
              frequentItemSets += (oneSizeSet -> 1)
            }
          }
        }
      }
    }
    for {p <- frequentItemSets.iterator} yield ((p._1.size, p._1), p._2)
  }
}
