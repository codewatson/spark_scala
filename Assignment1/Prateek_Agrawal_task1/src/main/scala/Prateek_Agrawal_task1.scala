import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext




object sort_avg_rat {
  def dropheader(data: RDD[String]):RDD[String]={
    data.mapPartitionsWithIndex((idx,lines)=>{
    if (idx == 0) {
    lines.drop(1)
    }
    lines
    })
  }
  case class Person(movID: Int, rating_avg: Double)

  def main(args: Array[String]) {
    val ratings = "ratings.csv"

    val conf = new SparkConf().setAppName("Prateek_Agrawal_task1").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val textFile = sc.textFile(ratings, 2).cache()
    val withoutHeader=dropheader(textFile).map(line => line.split(",")).map(x => (x(1) , x(2)) )

    val finalResult=withoutHeader.map{case (movid,movrat)=>(movid.toInt, (movrat.toDouble,1.0)) }
      .reduceByKey((x,y)=>(x._1 + y._1, x._2+y._2))
      .mapValues(sumcount=>sumcount._1/sumcount._2).sortByKey(true).repartition(1).foreach(println)

    sc.stop()
  }
}
