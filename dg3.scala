package dg.dg3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.storage.StorageLevel
import scribe.*
import scribe.format.*

import scala.util.chaining.*

object dg31:

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("Example Application")
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.*
        import io.github.pashashiz.spark_encoders.TypedEncoder.*
        import io.github.pashashiz.spark_encoders.TypedEncoder.given


        case class User(id: String) derives TypedEncoder
        case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt) derives TypedEncoder

//
//        val users = spark.createDataset(List(User("John"), User("Smith")))
//
//        users
//            .show()
//
//        val flightData2015 = spark
//            .read
//            .option("inferSchema", "true")
//            .option("header", "true")
//            .csv("data/flight-data/csv/2015-summary.csv")
//
//        flightData2015
//            .as[Flight]
//            .map(flight => flight.DEST_COUNTRY_NAME).toDF("DEST_COUNTRY_NAME")
//            .show()


//        spark
//            .range(10)
//            .rdd
//            .tap { rdd => println(s"the RDD type is: ${rdd.toDebugString}") }
//            .getNumPartitions
//            .pipe { n => println(s"\nthe number of partition is: $n\n") }


//        val sc      = spark.sparkContext
//        val base    = sc.parallelize(1 to 9, 3).toDS()
//        val squared = base.map(x => {println(s"map $x"); x * x})
//        val buckets = squared.groupByKey(x => x % 2).mapGroups((k, it) => (k, it.toList)) // 2 shuffle partitions
//        buckets.collect().foreach(println)


        case class Activity(userId: String,
                            cartId: String,
                            itemId: String)

        val activities = Seq(
            Activity("u1", "c1", "i1"),
            Activity("u1", "c1", "i2"),
            Activity("u2", "c2", "i1"),
            Activity("u3", "c3", "i3"),
            Activity("u4", "c4", "i3")
        )

        val ds = activities.toDS().persist(StorageLevel.MEMORY_ONLY)

        ds.show()

        ds.printSchema()

//        val users = ds.map(_.userId).toDF("userId").persist(StorageLevel.MEMORY_ONLY)
//
//        users.show()
//
//        users
//            .distinct()
//            .count()
//            .pipe(println)



        Thread.sleep(Int.MaxValue)

        spark.stop()

