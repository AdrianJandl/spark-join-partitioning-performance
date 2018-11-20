import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

import scala.collection.mutable
import scala.reflect.ClassTag

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Usage:")
      println("spark-submit jarfile.jar BIGDATA SMALLDATA REPETITIONS OUTPUTFILE")
      println("Where BROADCAST is trues to use broadcast hash join and false to not use it.")
      System.exit(1)
    }
    val bigDataPath = args(0)
    val smallDataPath = args(1)
    val repetitions = args(2).toInt
    val partitions = Array(48, 96, 144, 192)
    val outputPath = args(3)

    val sparkSession: SparkSession = SparkSession.builder().appName("DataFrame join").getOrCreate()
    for (part <- partitions) {
      //Dataset join
      datasetJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, part, "autoBroadcast")
      //RDD join
      rddJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, part)
      //RDD manual broadcast hash join
      manualBroadCastHashJoin(bigDataPath, smallDataPath, sparkSession, repetitions, part, outputPath)
      //DataFrame Join
      dataFrameJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, part, "autoBroadcast")
    }

    sparkSession.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    for (part <- partitions) {
      //DataFrame join
      dataFrameJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, part, "noAutoBroadcast")
      //Dataset join
      datasetJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, part, "noAutoBroadcast")
    }
  }

  def datasetJoin(sparkSession: SparkSession, repetitions: Int, bigDataPath: String, smallDataPath: String, outputPath: String, partitions: Int, appendix: String): Unit = {
    val results = mutable.MutableList[String]()

    results.++=(doDatasetJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions, appendix))
    results.++=(doDatasetJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions, appendix))
    results.++=(doDatasetJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions, appendix))
    results.++=(doDatasetJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions, appendix))
    results.++=(doDatasetJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions, appendix))

    printOutput(outputPath, sparkSession, results, "dataset-" + partitions + "-" + appendix)
  }

  def doDatasetJoin(sparkSession: SparkSession, repetitions: Int, bigDataPath: String, smallDataPath: String,
                    outputPath: String, partitions: Int, appendix: String): mutable.MutableList[String] = {
    val results = mutable.MutableList[String]()
    import sparkSession.sqlContext.implicits._
    val bigData: Dataset[Geoname] = sparkSession
      .read
      .option("header", "true")
      .schema(Encoders.product[Geoname].schema)
      .option("mode", "DROPMALFORMED")
      .csv(bigDataPath)
      .as[Geoname]

    val metrics = mutable.MutableList[String]()
    metrics.+=("Number of workers: " + (sparkSession.sparkContext.getExecutorStorageStatus.length - 1))
    metrics.+=("Cores: " + Runtime.getRuntime.availableProcessors())

    val partitionedBig = if (partitions > 0) bigData.repartition(partitions) else bigData
    metrics.+=("Partitions:>" + partitionedBig.rdd.getNumPartitions + "<")
    results.++=(metrics)

    val smallData = sparkSession
      .read
      .option("header", "true")
      .schema(Encoders.product[Timezone].schema)
      .option("mode", "DROPMALFORMED")
      .csv(smallDataPath)
      .as[Timezone]
    //force the two encoders to be instantiated before taking timing to avoid first run taking significantly longer
    bigData.count()
    smallData.count()

    val start = System.nanoTime()
    val joined = partitionedBig.join(smallData, "country_code")
    joined.count()
    val end = System.nanoTime()
    results.+=((end - start).toString)
    results
  }

  def rddJoin(sparkSession: SparkSession, repetitions: Int, bigDataPath: String, smallDataPath: String, outputPath: String, partitions: Int): Unit = {
    val results = mutable.MutableList[String]()

    results.++=(doRddJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions))
    results.++=(doRddJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions))
    results.++=(doRddJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions))
    results.++=(doRddJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions))
    results.++=(doRddJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions))

    printOutput(outputPath, sparkSession, results, "rdd-" + partitions)
  }

  def doRddJoin(sparkSession: SparkSession, repetitions: Int, bigDataPath: String, smallDataPath: String, outputPath: String, partitions: Int): mutable.MutableList[String] = {
    val results = mutable.MutableList[String]()
    val bigData: RDD[(String, Iterable[String])] = sparkSession
      .sparkContext
      .textFile(bigDataPath, partitions)
      .map(_.split(","))
      .map(item => (item(8), item(1)))
      .groupByKey()
    val smallData: RDD[(String, Iterable[String])] = sparkSession
      .sparkContext.
      textFile(smallDataPath)
      .map(_.split(","))
      .map(item => (item(0), item(1)))
      .groupByKey()

    val metrics = mutable.MutableList[String]()
    metrics.+=("Number of workers: " + (sparkSession.sparkContext.getExecutorStorageStatus.length - 1))
    metrics.+=("Cores: " + Runtime.getRuntime.availableProcessors())
    metrics.+=("Partitions:>" + bigData.getNumPartitions + "<")
    results.++=(metrics)

    val start = System.nanoTime()
    val joined: RDD[(String, (Iterable[String], Iterable[String]))] = bigData.join(smallData)
    joined.count()
    val end = System.nanoTime()
    results.+=((end - start).toString)
    results
  }

  def dataFrameJoin(sparkSession: SparkSession, repetitions: Int, bigDataPath: String, smallDataPath: String, outputPath: String, partitions: Int, appendix: String): Unit = {
    val results = mutable.MutableList[String]()

    results.++=(doDataFrameJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions,
      appendix))
    results.++=(doDataFrameJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions,
      appendix))
    results.++=(doDataFrameJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions,
      appendix))
    results.++=(doDataFrameJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions,
      appendix))
    results.++=(doDataFrameJoin(sparkSession, repetitions, bigDataPath, smallDataPath, outputPath, partitions,
      appendix))

    printOutput(outputPath, sparkSession, results, "dataFrame-" + partitions + "-" + appendix)
  }

  def doDataFrameJoin(sparkSession: SparkSession, repetitions: Int, bigDataPath: String, smallDataPath: String, outputPath: String, partitions: Int, appendix: String): mutable.MutableList[String] = {
    val results = mutable.MutableList[String]()
    val bigData: DataFrame = sparkSession
      .read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(bigDataPath)

    val metrics = mutable.MutableList[String]()
    metrics.+=("Number of workers: " + (sparkSession.sparkContext.getExecutorStorageStatus.length - 1))
    metrics.+=("Cores: " + Runtime.getRuntime.availableProcessors())
    val partitionedBigData = if (partitions > 0) bigData.repartition(partitions) else bigData
    metrics.+=("Partitions:>" + partitionedBigData.rdd.getNumPartitions + "<")
    results.++=(metrics)

    partitionedBigData.count()

    val smallData: DataFrame = sparkSession
      .read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(smallDataPath)

    val start = System.nanoTime()
    val joined: DataFrame = partitionedBigData.join(smallData, "country code")
    joined.explain(true)
    joined.count()
    val end = System.nanoTime()
    results.+=((end - start).toString)
    results
  }

  def manualBroadCastHashJoin(bigDataPath: String, smallDataPath: String, sparkSession: SparkSession, repetitions: Int, partitions: Int, outputPath: String): Unit = {
    val results = mutable.MutableList[String]()
    results.++=(doManualBroadcastHashJoin(bigDataPath, smallDataPath, sparkSession, repetitions, partitions, outputPath))
    results.++=(doManualBroadcastHashJoin(bigDataPath, smallDataPath, sparkSession, repetitions, partitions, outputPath))
    results.++=(doManualBroadcastHashJoin(bigDataPath, smallDataPath, sparkSession, repetitions, partitions, outputPath))
    results.++=(doManualBroadcastHashJoin(bigDataPath, smallDataPath, sparkSession, repetitions, partitions, outputPath))
    results.++=(doManualBroadcastHashJoin(bigDataPath, smallDataPath, sparkSession, repetitions, partitions, outputPath))
    printOutput(outputPath, sparkSession, results, "rdd-manual-bhj-" + partitions)
  }

  def doManualBroadcastHashJoin(bigDataPath: String, smallDataPath: String, sparkSession: SparkSession,
                                repetitions: Int, partitions: Int, outputPath: String): mutable.MutableList[String] = {
    val results = mutable.MutableList[String]()
    val bigRDD: RDD[(String, Iterable[String])] = sparkSession
      .sparkContext
      .textFile(bigDataPath, partitions)
      .map(_.split(","))
      .map(item => (item(8), item(1)))
      .groupByKey()
    val smallRDD: RDD[(String, Iterable[String])] = sparkSession
      .sparkContext.
      textFile(smallDataPath)
      .map(_.split(","))
      .map(item => (item(0), item(1)))
      .groupByKey()

    val metrics = mutable.MutableList[String]()
    metrics.+=("Number of workers: " + (sparkSession.sparkContext.getExecutorStorageStatus.length - 1))
    metrics.+=("Cores: " + Runtime.getRuntime.availableProcessors())
    metrics.+=("Partitions:>" + bigRDD.getNumPartitions + "<")
    results.++=(metrics)

    val timeTaken = manualBroadcastHashJoin(bigRDD, smallRDD).toString

    results.+=(timeTaken)
    results
  }


  def manualBroadcastHashJoin[K: Ordering : ClassTag, V1: ClassTag, V2: ClassTag](bigRDD: RDD[(K, V1)], smallRDD: RDD[(K, V2)]): Long = {
    val smallRDDLocal = smallRDD.collectAsMap()
    val bc: Broadcast[collection.Map[K, V2]] = bigRDD.sparkContext.broadcast(smallRDDLocal)
    bigRDD.count()

    val start = System.nanoTime()
    val joined = bigRDD.mapPartitions(iter => {
      iter.flatMap {
        case (k, v1) =>
          bc.value.get(k) match {
            case None => Seq.empty[(K, (V1, V2))]
            case Some(v2) => Seq((k, (v1, v2)))
          }
      }
    }, preservesPartitioning = true)
    joined.count()
    val end = System.nanoTime()
    end - start
  }

  def printOutput(outputPath: String, sparkSession: SparkSession, results: mutable.MutableList[String], appendix: String): Unit = {
    if (outputPath.isEmpty) {
      val logger = Logger.getRootLogger
      results.foreach(logger.info(_))
    } else {
      sparkSession.sparkContext.parallelize(results).coalesce(1).saveAsTextFile(outputPath + appendix)
    }
  }

}
