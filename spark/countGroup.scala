
import spark.implicits._ 
import spark.sql

val inputFile = sc.textFile("/work/charCount.txt")
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
case class dup(name: String, value: Int)

val dupDF = inputFile.map(_.split(",")).map(e => dup(e(0).trim, e(1).trim.toInt)).toDF()
dupDF.registerTempTable("dupsTable")
dupDF.groupBy("name").sum("value").withColumnRenamed("name","Name").withColumnRenamed("sum(value)","Sum").show()