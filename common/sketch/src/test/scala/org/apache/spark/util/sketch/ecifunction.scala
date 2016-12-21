package com.optum.bds.brms.spark

class ECIFunctions extends Serializable{
    val dateformat = new java.text.SimpleDateFormat("yyyy-MM-dd-HH:mm:ss.SSSSSS")
  def getDate(): String = {
    val calendar = java.util.Calendar.getInstance()
    val tmp = new java.sql.Timestamp(calendar.getTimeInMillis())
    return dateformat.format(tmp)
  }
  def getTimeDifference(startTime: String, endTime: String): String = {
    val t1 = dateformat.parse(startTime).getTime
    val t2 = dateformat.parse(endTime).getTime
    val timeDiff = t2 - t1
    val secDiff = timeDiff / 1000 % 60
    val minDiff = timeDiff / (60 * 1000) % 60
    val hourDiff = timeDiff / (60 * 60 * 1000) % 24
    return "Hours: " + hourDiff + ", Minutes: " + minDiff + ",Seconds: " + secDiff
  }

  def cleanOutputPath(outputNodeAddress: String, outputFilePath: String): Unit = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(outputNodeAddress), hadoopConf)
    val path = new org.apache.hadoop.fs.Path(outputFilePath)
    if (hdfs.exists(path)) {
      println("deleting output directory from path : " + path)
      try { hdfs.delete(path, true) } catch { case _: Throwable => {} }
    } else { printf("output directory from path %s is empty.... Exiting clean", path) }
  }

  def stringreplace(database: String, recordid: String, big4: String, query: String): (String, (String, String)) = {
    val big4arr = big4.split("~")
    val partn_nbr = big4arr.slice(0, 1)(0)
    val cnsm_id = big4arr.slice(1, 2)(0)
    val src_cd = big4arr.slice(2, 3)(0)
    val lgcy_src_id = big4arr.slice(3, 4)(0)
    val finalquery = (recordid, query.replaceAll("%SCHEMA%", database).replaceAll("%CNSM_ID%", cnsm_id).replaceAll("%PARTN_NBR%", partn_nbr).replaceAll("%SRC_CD%", "\\\"" + src_cd + "\\\"").replaceAll("%LGCY_SRC_ID%", "\\\"" + lgcy_src_id + "\\\""))
    val x = (big4, finalquery)
    return x
  }
}
