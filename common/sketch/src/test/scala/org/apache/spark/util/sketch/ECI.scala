package com.optum.bds.brms.spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import scala.collection.immutable.Set
import org.apache.commons.lang.mutable.Mutable
import scala.util.DynamicVariable
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

//case class inputschema(partn_nbr: String, cnsm_id: String, src_cd: String, lgcy_src_id: String)

object ECIBuilder {

  def main(args: Array[String]) {
    var runType = "FILE"
    if (args.length == 1) {
      runType = args(0)
    }

    val arglist = args.toList

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val hcont = new HiveContext(sc)
    import hcont.implicits._


    val hdfsOutputFilePath = sc.getConf.get("spark.hdfs.outputFilePath")
    val inputFilePath = sc.getConf.get("spark.inputFilePath")
    val queryStorePath = sc.getConf.get("spark.queryStorePath")
    val OBDPquery = sc.getConf.get("spark.OBDPquery")
    val BRMSquery = sc.getConf.get("spark.BRMSquery")

    val eci_rec_typ_df = hcont.read.table("cdb.eci_rec_typ_orc")
    val f = org.apache.spark.sql.functions
    val rec_types = eci_rec_typ_df.where(eci_rec_typ_df("eci_rec_ces_ind") === "Y").select("ecirectypk")
    val rec_t = rec_types.select(f.collect_set(rec_types("ecirectypk")))
    val r = rec_t.first.get(0).asInstanceOf[Seq[String]].toSet

    val queryStore = sc.textFile(queryStorePath)
    val schemaString = "partn_nbr,cnsm_id,src_cd,lgcy_src_id"
    val fields = schemaString.split(',').map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    var query = ""
    var outputLoc = hdfsOutputFilePath + "/fileout"
    val inputFile = sc.textFile(inputFilePath)
    val inputRDD = inputFile.map {
        inputLine =>
          val linearr = inputLine.split(",")
          val arrlen = (linearr).length
          val partn_nbr = linearr.slice(0, 1).mkString("")
          val cnsm_id = linearr.slice(1, 2).mkString("")
          val src_cd = linearr.slice(2, 3).mkString("")
          val lgcy_src_id = linearr.slice(3, 4).mkString("")
          Row(partn_nbr, cnsm_id, src_cd, lgcy_src_id) // => inputschema(partn_nbr,cnsm_id,src_cd,lgcy_src_id,recType)
      }
    var feed = hcont.createDataFrame(inputRDD, schema)
    if (runType == "OBDP") {
      query = OBDPquery
      outputLoc = hdfsOutputFilePath + "/obdpout"
      feed = hcont.sql(query)
    }else if (runType == "BRMS") {
      query = BRMSquery
      outputLoc = hdfsOutputFilePath + "/brmsout"
      feed = hcont.sql(query)
    }
    feed.count()
    feed.cache()

    println("Running ECI Builder for application: " + runType)


    var dm = List[String]()
    var dk = collection.mutable.Map.empty[String, DataFrame]
    //val processedL = List("100")
    queryStore.collect.foreach {
      queryWithRecid =>
        val queryRecidSplits = queryWithRecid.split('\u0001')
        println(queryRecidSplits(0))
        // (!processedL.contains(queryRecidSplits(0))){
        if (r.contains(queryRecidSplits(0))) {
          println("Adding " + queryRecidSplits(0))
          //println(queryRecidSplits(3))
          dm = queryRecidSplits(0) :: dm
          val tempdf = hcont.sql(queryRecidSplits(3))
          val joined = feed.as("df1").join(tempdf.as("df2"),
            f.trim($"df1.partn_nbr") === f.trim($"df2.partn_nbr") &&
              f.trim($"df1.cnsm_id") === f.trim($"df2.cnsm_id") &&
              f.trim($"df1.src_cd") === f.trim($"df2.src_cd") &&
              f.trim($"df1.lgcy_src_id") === f.trim($"df2.lgcy_src_id"), "inner").
            select("df1.partn_nbr", "df1.cnsm_id", "df1.src_cd", "df1.lgcy_src_id", "df2.rec_typ", "df2.layout").sort("df1.partn_nbr", "df1.cnsm_id", "df1.src_cd", "df1.lgcy_src_id").select("layout")
          dk += (queryRecidSplits(0) -> joined)
          //println("Done with " + queryRecidSplits(0) + "=========")
        } else {
          println(queryRecidSplits(0) + " is not a right ECI type")

          //}
        }
    }

    val schemaString2 = "layout"
    val fields2 = schemaString2.split(',').map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema2 = StructType(fields2)
    var finalDF = hcont.createDataFrame(sc.emptyRDD[Row], schema2)
    val typeL = dm.sortWith(_ < _)

    typeL.foreach {
      curType =>
        finalDF = finalDF.unionAll(dk(curType))
    }


    finalDF.filter("layout is not null").coalesce(1).map { x => x(0) }.saveAsTextFile(outputLoc)

    println("Done")

    sc.stop()
  }
}

