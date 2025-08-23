package sparktypes

import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.classic.ColumnConversions.toRichColumn
import org.apache.spark.sql.functions.*

import io.github.pashashiz.spark_encoders.*
import io.github.pashashiz.spark_encoders.TypedEncoder.*


import scribe.*
import scribe.format.*
import scala.util.chaining.*
import scala.jdk.CollectionConverters.*


/**
 *
 *
 * === Range as a Relation Expression ===
 *
 *
 */

object SparkTypes11:

    Logger.root
          .clearHandlers()
          .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
          .replace()

    case class Person(name: String, age: Int, birthday: Option[Int])
    case class PersonXML(xml: String)
    case class Record(complex: (Long, Long), id: Long, x: Long, y: Long)

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
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        spark
            .createDataset(List(Person("John", 30, None), Person("Smith", 40, Some(2))))
            .tap { ds => ds.schema.printTreeString() }
            .tap { ds => ds.explain(true) }
            .tap { ds => ds.show(truncate = false) }


        spark
            .range(1) // <-- this is a dataset of Long with a column named id
            .select(col("*"), lit(1).as("x")) // <-- this adds a column named x with a value of 1 to each row
            .tap { _.schema.printTreeString() }
            .tap { _.explain(true) }
            .tap { _.show(truncate = false) }



        spark
            .range(1) // <-- this is a dataset of Long with a column named id
            .select($"id", lit(1).as("x"))  // <-- this adds a column named x with a value of 1 to each row
            .selectExpr("(id, x) as complex", "*") // <-- same as struct($"id", $"x")
            .withColumn("y", $"x" + 1)
            .as[Record]
            .tap { _.schema.printTreeString() }
            .tap { _.explain(true) }
            .tap{ _.show(truncate = false)}



        // Demo XML serialization

        // Demo deserialization of one field value into a case class
        // Not a typical thing to do - case class are for structured information

        spark
            .createDataset(List(Person("John", 30, None), Person("Smith", 40, Some(2))))
            .select(struct($"*") as "person")
            .select(to_xml($"person", Map("rowTag" -> "Person").asJava) as "person_xml")
            .persist() // <-- cache to not re-compute later
            .tap { _.schema.printTreeString() }
            .tap { _.explain(true) }
            .tap { _.show(truncate = false) }
            .withColumnRenamed("person_xml", "xml") // <-- make the field match the case class only field
            .as[PersonXML]  // <-- Contrived - A case class to hold the xml string - just for the sake of deserialization example
            .tap { _.schema.printTreeString() }
            .tap { _.explain(true) }
            .tap { _.show(truncate = false) }
            .collect()
            .toList
            .map(_.xml)
            .foreach(println)


        Thread.sleep(Int.MaxValue)

        spark.stop()
    }


object SparkTypes12:

    Logger.root
          .clearHandlers()
          .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
          .replace()

    sealed trait Person
    case class Adult(name: String, age: Int, birthday: Option[Int]) extends Person
    case class Child(name: String, age: Int, birthday: Option[Int], guardian: String) extends Person
    case class Senior(name: String, age: Int, birthday: Option[Int], pensionId: String) extends Person

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("Example Application")
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        // import spark.implicits._
        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.*

        val adultsDF = List(Adult("John", 30, None), Adult("Jane", 25, Some(1998))).toDF()
        val childrenDF = List(Child("Billy", 10, Some(2015), "John")).toDF()
        val seniorsDF = List(Senior("George", 70, Some(1955), "SN123456")).toDF()


        val adultsDS = adultsDF.as[Adult].map(identity[Person])
        val childrenDS = childrenDF.as[Child].map(identity[Person])
        val seniorsDS = seniorsDF.as[Senior].map(identity[Person])

        val persons = adultsDS.unionByName(childrenDS).unionByName(seniorsDS)




        persons.schema.printTreeString()

        persons.show()


/**
 * == Modeling Entity Change as ADT using Encoder Library ==
 */
object SparkTypes2:

    Logger.root
          .clearHandlers()
          .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
          .replace()


    sealed trait Person
    case class Adult(name: String, age: Int, birthday: Option[Int]) extends Person
    case class Child(name: String, age: Int, birthday: Option[Int], guardian: String) extends Person
    case class Senior(name: String, age: Int, birthday: Option[Int], pensionId: String) extends Person

    sealed trait Change[+T](entity: T)
    case class Insert[T](entity: T) extends Change[T](entity)
    case class Update[T](entity: T) extends Change[T](entity)
    case class Delete[T](entity: T) extends Change[T](entity)


    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("Example Application")
            .master("local[*]")
            .getOrCreate()

    def main (args: Array[String]): Unit =

        val spark = makeSparkSession

        //import spark.implicits._
        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.*

        val insertAdult  = Insert(Adult("John", 30, None))
        val deleteAdult  = Delete(Adult("John", 30, None))
        val insertChild  = Insert(Child("Billy", 10, None, "John"))
        val insertSenior = Insert(Senior("George", 70, None, "SN123456"))


        List[Change[Person]](insertAdult, deleteAdult, insertChild, insertSenior)
            .toDS()
            .tap { _.schema.printTreeString() }
            .tap { _.explain(true) }
            .tap { _.show(truncate = false) }
            .collectAsList()
            .asScala
            .toList
            .foreach(println)