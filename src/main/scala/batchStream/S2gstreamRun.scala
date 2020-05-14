package org.clustering4ever.spark.clustering

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._

import smile.validation.{Accuracy, AdjustedRandIndex, MutualInformationScore}


object S2gstreamRun extends App{


  override def main(args: Array[String]) {

    val master = "local[2]"
    val dirData = "streams"
    val dirSortie = "results"
    val path =   "C://Users/ATTAOUI/Documents/SS_GStream/resources/DS1.txt"
    val separator = ","
    val decayFactor = 0.99
    val lambdaAge = 1.2
    val nbNodesToAdd = 3
    val nbWind = 60



  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sparkConf = new SparkConf().setAppName(this.getClass().getName())
  sparkConf.setMaster(master)

  val sc = new SparkContext(sparkConf)
  val interval = 100
  val batchInterval = Milliseconds(interval)
  val ssc = new StreamingContext(sc, batchInterval)

  // 'points2' contains the first two data-points used for initialising the model

  val data = sc.textFile(path).map(x => x.split(',').map(_.toDouble))
  val v = data.take(1)(0).length
  val data2 = data.collect()
  var labels =  data.map(x => x(v - 1).toInt).collect()


  val points2 = sc.parallelize(data2.slice(0, 2)) //sc.textFile("resources/nodes2.txt").map(x => x.split(separator).map(_.toDouble))
  var batchNumber = 1
  var batchIndex = 0
  val batchSize = 100
  //var stream = new Array[Array[Double]](0)
  var stream2 = new Array[streamData](0)


  // Create a DStreams that reads batch files from dirData
  val stream = ssc.textFileStream(dirData).map(x => x.split(separator).map(_.toDouble))
  //Create a DStreams that will connect to a socket hostname:port

  val labId = 2 //TODO: change -1 to -2 when you add the id to the file (last column) //-2 because the last 2 columns represent label & id
  val dim = points2.take(1)(0).size - labId
  var gstream = new S2gstream()
    .setDecayFactor(decayFactor)
    .setLambdaAge(lambdaAge)
    .setMaxInsert(nbNodesToAdd)
  // converting each point into an object
  val dstreamObj = stream.map( e =>
    gstream.model.pointToObjet(e, dim, labId)
  )

  while(batchNumber <= (data2.length / batchSize) ){
    val batch = data2.slice(batchIndex, batchIndex + batchSize)
    stream2 :+= new streamData(batch.map(a => gstream.model.pointToObjet(a, dim, labId)))
    batchIndex += batchSize
    batchNumber += 1
  }

  dstreamObj.cache() //TODO: to save in memory

  // initialization of the model by creating a graph of two nodes (the first 2 data-points)
  gstream.initModelObj(points2, dim)

  // training on the model
    val streamRDD = stream2.map(x => sc.parallelize(x.stream))

    val trainedModel = gstream.trainOnObj(dstreamObj, gstream, dirSortie + "/dataset" + nbNodesToAdd, dim, nbWind, streamRDD, labels, data2)
  val t0 = System.currentTimeMillis()

  //ssc.start()

  //ssc.awaitTermination() //stop(true, true)
   val t1 = System.currentTimeMillis()
  println("Elapsed time: " + (t1 - t0) + "ns")
  }


}
