import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext



object sort_tag_rat {
  def dropheader(data: RDD[String]):RDD[String]={
    data.mapPartitionsWithIndex((idx,lines)=>{
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }
  case class Person(tag: String, rating_avg: Double)

  def main(args: Array[String]) {

    val ratings = "ratings.csv"
    val tags = "tags.csv"

    val conf = new SparkConf().setAppName("RatingsAggregate").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val ratingsRdd = sc.textFile(ratings, 2).cache()
    val tagsRdd = sc.textFile(tags, 2).cache()

    val withoutHeaderRatingsRdd=dropheader(ratingsRdd).map(line => line.split(",")).map(x => (x(1) , x(2)) )
    val withoutHeaderTagsRdd=dropheader(tagsRdd).map(line => line.split(",")).map(x => (x(1).toInt , x(2)) )


    val avg_rat=withoutHeaderRatingsRdd.map{case (movid,movrat)=>(movid.toInt, (movrat.toDouble,1.0)) }.reduceByKey((x,y)=>(x._1 + y._1, x._2+y._2)).mapValues(sumcount=>sumcount._1/sumcount._2).sortByKey(true)


    val joinedRdd = withoutHeaderTagsRdd.join(avg_rat)

    val finalResult=joinedRdd.map{case (movid,(movtag,movrat))=>(movtag,(movrat.toDouble,1.0))}.reduceByKey((x,y)=>(x._1 + y._1, x._2+y._2)).mapValues(sumcount=>sumcount._1/sumcount._2).sortByKey(false)



    val df=finalResult.map(p => Person(p._1, p._2)).toDF()
    df.registerTempTable("df")
//    df.coalesce(1).select("tag", "rating_avg").write.format("com.databricks.spark.csv").option("header","true").save("/Users/echoesofconc/Documents/USC_courses/INF553/ml-latest-small/tags_avg.csv")
    df.coalesce(1).select("tag", "rating_avg").write.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header","true").save("tags_avg.csv")

    sc.stop()
  }
}
