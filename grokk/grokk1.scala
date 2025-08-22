package grokk.grokk1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.streaming.*

//import org.apache.spark.sql.types.*
//import org.apache.spark.sql.functions._

object ProgramLogic:

    def query(spark: SparkSession): Unit =
        import spark.implicits._
        import org.apache.spark.sql.functions.col

        Seq(4, 2, 3)
          .toDS
          .groupByKey( _ % 2)
          .count()
          .toDF("key", "count") //instead of count(1) as col name for the count
          .show()



    def makeSparkSession: SparkSession =
        SparkSession
          .builder()
          .appName("Example Application")
          .master("local[*]")
          .getOrCreate()


    def runSparkQuery: SparkSession => (SparkSession => Unit) => Unit =
        ss => program =>
            program(ss)


@main
def main(): Unit =

    import ProgramLogic.*

    val spark = makeSparkSession

    //println(spark.conf.get("spark.hadoop.fs.s3.impl"))
    //println(spark.conf.get("spark.hadoop.fs.s3a.impl"))


    runSparkQuery(spark)(query)

    println(SparkSession.active.version)

    spark.stop()


