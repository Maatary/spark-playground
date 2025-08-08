package sparktypes

import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.classic.ColumnConversions.toRichColumn
import org.apache.spark.sql.functions.*

import io.github.pashashiz.spark_encoders.*
import io.github.pashashiz.spark_encoders.TypedEncoder.*
import io.github.pashashiz.spark_encoders.TypedEncoder.given

import scribe.*
import scribe.format.*
import scala.util.chaining.*
import scala.jdk.CollectionConverters.*

object DataTypes:
    case class Person(name: String, age: Int, birthday: Option[Int])
    case class Record(complex: (Long, Long), id: Long, x: Long, y: Long)

object SparkTypes:

    import DataTypes.*

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("Example Application")
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit = {

        val spark = makeSparkSession

        // import spark.implicits._
        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}

//        spark
//            .createDataset(List(Person("John", 30, None), Person("Smith", 40, Some(2))))
//            .schema.sql
//            .pipe(println)

        // val enc = TypedEncoder[Person].encoder
        // enc.schema.sql.pipe(println)

//        spark
//            .range(1)
//            .select(col("*"), lit(1).as("x"))
//            .show()

//        spark
//            .range(1)
//            .select($"id", lit(1).as("x"))
//            .selectExpr("(id, x) as complex", "*")
//            .withColumn("y", $"x" + 1)
//            .as[Record]
//            .schema
//            .printTreeString()
//            //.show()

        spark
            .createDataset(List(Person("John", 30, None), Person("Smith", 40, Some(2))))
            .select(struct($"*") as "person")
            .select(to_xml($"person", Map("rowTag" -> "Person").asJava) as "person_xml")
            .as[String]
            .collect()
            .toList
            .foreach(println)


        spark.stop()
    }
