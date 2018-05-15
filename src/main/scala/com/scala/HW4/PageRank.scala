package com.scala.HW4
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.storage.StorageLevel

/**
 * Calculates page rank for 10 iterations and
 * returns top K pages in decreasing order of page rank
 * @author fibinfa
 *
 */
object PageRank {
  def main(args: Array[String]) = {
    val startTime = System.nanoTime();
    //initialise config - "spark master = yarn for EMR exec, local for local execution"
    val conf = new SparkConf()
      .setAppName("Page Rank")
      .setMaster("yarn")
    //initialise the Spark context
    val sc = new SparkContext(conf)

    //accumulator values stored
    //total no. of pages
    val pageCount = sc.accumulator(0, "pagecount")
    //dangling node contribution
    val delta = sc.accumulator(0.toDouble, "delta")
    //damping factor
    val alpha = sc.broadcast(0.15)
    //top k results, k in this case is 100
    val k = 100

    val startTimePre = System.nanoTime()

    // preprocess() returns RDD with key as page name, value as adj list
    // persist is used so that it is cached in memory and
    //each time we dont have to compute again
    val pageWithAdjList = preprocess(sc, args(0))
      .persist(StorageLevel.MEMORY_AND_DISK);
    val endTimePre = System.nanoTime()
    //count number of pages and dangling nodes
    //initailise page rank as negative infinity
    var pageWithRanks = pageWithAdjList.map(t => {
      pageCount += 1
      (t._1, Double.NegativeInfinity)
    })

    //force this using an action
    val count = pageWithRanks.count()

    //initialisation

    val initialPageRank = sc.broadcast(1 / pageCount.value.toDouble)
    val totalPages = sc.broadcast(pageCount.value)
    val alphaByPageCount = sc.broadcast(alpha.value / pageCount.value)

    //---------------------page rank calculation-----------------------------
    //10 iterations
    val startTimePR = System.nanoTime()
    val i = 0
    for (i <- 0 to 10) {
      val pageWithAdjListAndPR = pageWithAdjList.join(pageWithRanks)

      //calculate 2nd term that is contribution of inlinks
      val inlinkCont = pageWithAdjListAndPR.flatMap(page => {
        var outlinks = Array.empty[String]
        var outlinksBuffer = new ListBuffer[String]
        var intermediateRank = 0.toDouble
        var prevRank = initialPageRank.value
        if (page._2._2 != Double.NegativeInfinity) {
          // Means that this is not the first iteration
          prevRank = page._2._2
        }
        if (page._2._1 != null && page._2._1 != "" && page._2._1.length() > 0) {
          outlinks = page._2._1.split(",")
          for (outlink <- outlinks) {
            outlinksBuffer += outlink
          }
          intermediateRank = prevRank
          intermediateRank = prevRank / outlinks.length

        } else {
          //dangling node - so delta gets added
          delta += prevRank
        }
        //this step is performed inorder to handle the special case of
        //a node appearing in the key but not in the adj list of any nodes
        outlinksBuffer += page._1

        //send fractional outlink to each outlink and then reduce by key
        //is performed which will give us the sum
        outlinksBuffer.map(outlink => {
          if (outlink != page._1) {
            (outlink, intermediateRank)
          } else {
            (outlink, 0.toDouble)
          }
        })
        //reduce by key is used to get the sum of values
      }).reduceByKey(_ + _)

      //new value of delta is calculated and broadcasted
      val newDelta = sc.broadcast(delta.value)

      //calculate the new pagerank from intermediate ranks, i,e, 2nd term in page rank formula
      pageWithRanks = inlinkCont.map(page => {
        //dangling node contribution
        val a = newDelta.value / totalPages.value
        // 1- alpha
        val b = 1 - alpha.value
        //2nd term + 3rd term i.e. inlink contribution + dangling node
        // contribution
        val c = a + page._2
        (page._1, alphaByPageCount.value + b * c)
      })

      delta.setValue(0)

    }
    val endTimePR = System.nanoTime()

    // --------------------top100------------------------------------------

    //Find top K after 10th iteration
    //ordering is done on page rank value
    val topK = sc.parallelize(pageWithRanks.top(k)(Ordering[(Double)].on(page => page._2)), 1)

    topK.saveAsTextFile(args(1))

    val endTime = System.nanoTime();

    println("total time: " + (endTime - startTime))
    println("total time for PR calc: " + (endTimePR - startTimePR))
    println("total time for Preprocessing: " + (endTimePre - startTimePre))

  }

  //--------------------PreProcessing------------------------------------------------

  /**
   * preprocessing step which processes the inpu file
   * and gives an rdd with page name and corresponding adj list
   * @param sc - spark context
   * @param path - file stored here
   * @return - rdd which stores adj list
   */
  def preprocess(sc: SparkContext, path: String): RDD[(String, String)] = {
    //read file line by line
    val resultRDD = sc.textFile(path).map(line =>
      //parse line is called on each line of the document
      Bz2WikiParser.parseLine(line))
      //null values are eliminated
      .filter(line => line != null)
      //page name and adj list are separated
      .map(line => {
        val splits = line.split(": ")
        (splits(0), splits(1).substring(1, splits(1).length() - 1))
      }).flatMap(t => {

        val outlinkString = t._2

        var listBuffer = new ListBuffer[Tuple2[String, String]]
        //dangling nodes= []
        if (outlinkString != "" && outlinkString != null && outlinkString.length() > 0) {
          val outlinks = outlinkString.split(", ")
          //handle the case of page present in the outlink
          //but not in key
          for (outlink <- outlinks) {
            listBuffer += new Tuple2(outlink, "")
          }

          listBuffer += new Tuple2(t._1, outlinkString)
        } else {
          listBuffer += new Tuple2(t._1, "")
        }
        listBuffer.map(x => x)
        //flatmap will emit all keys along with their outlink as key and null as
        //value. reduce by key will group them by keys and values are appended
        //after ignoring the null values
        //reduce by key is used to make sure that no duplicate records are emitted
      }).reduceByKey((x, y) => {
        if (x != null && x != "" && x.length() > 0 &&
          y != null && y != "" && y.length() > 0) {
          x + ";" + y
        } else {
          x + y
        }
      })
    return resultRDD;
  }
}
