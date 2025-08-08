package hps.hps3

import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import scribe.*
import scribe.format.*

import scala.util.chaining.*
import io.github.pashashiz.spark_encoders.*
import io.github.pashashiz.spark_encoders.TypedEncoder.*
import io.github.pashashiz.spark_encoders.TypedEncoder.given
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.classic.ColumnConversions.toRichColumn
import org.apache.spark.sql.functions.*

object hps31:

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("Example Application")
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}

        case class RawPanda(id: Long, zip: String, pt: String, happy: Boolean, attributes: List[Double])
        case class PandaPlace(name: String, pandas: List[RawPanda])

        val panda1     = RawPanda(1, "M1B 5K7", "giant", true, List(0.1, 0.1))
        val panda2     = RawPanda(2, "M1B 5K6", "giant", true, List(0.2, 0.2))
        val panda3     = RawPanda(3, "X1B 5G7", "giant", false, List(10.0, 5.0))
        val panda4     = RawPanda(4, "M1B 5K7", "giant", false, List(0.1, 0.1))
        //val pandasInfo = List(panda1, panda2, panda3, panda4).toDF()

        val pandaPlace1  = PandaPlace("toronto", List(panda1, panda2))
        val pandaPlace2  = PandaPlace("newYork", List(panda3))
        val pandaPlaces = Seq(pandaPlace1, pandaPlace2).toDS()
        //pandaPlaces.printSchema()

        //TypedEncoder.apply[PandaPlace].encoder.schema.printTreeString() // can i do something similar with column

        //TypedEncoder.apply[PandaPlace].encoder.createSerializer()(pandaPlace1).toString pipe println
        implicitly[ExpressionEncoder[PandaPlace]]
        val enc = TypedEncoder.apply[PandaPlace].encoder
        val resolvedEnc = enc.resolveAndBind()
        val toRow = resolvedEnc.createSerializer()
        val internalRow = toRow(pandaPlace1)
        val fromRow = resolvedEnc.createDeserializer()
        fromRow(internalRow) pipe println

        //Encoders.product[PandaPlace].createSerializer()(pandaPlace1).toString pipe println

        //spark.emptyDataFrame.as[PandaPlace]




//        col("x").explain(true)
//        lit(1).explain(true)
//        lit("hello").as("x").explain(true)
//
//        //col("x").expr.dataType.sql pipe println
//
//        col("x").expr.sql pipe println
//
//        lit(1).as("x").expr.sql pipe println
//
//        lit(1).as("x").expr.toString() pipe println
//
//        lit(1).as[Int].expr.dataType.toString pipe println



        // even if we compare it is always column operator
        // so the result must be red from the colum
        // colum is a recipe to get the value at a particular column
        // if the outter function expect a boolean that that is what the column should contains
        // sql expression handle that internally

        // pandasInfo.filter("happy != true" ).show()  // sql expression version
        // pandasInfo.filter(col("happy") === lit(false)).show() //DF version
        // pandasInfo.filter($"happy" === true).coalesce(1).show() //DF version with implicit on both column name and the literral
        // pandasInfo.filter(($"id" > 0) and ($"id" <= 1)).show()

        // pandasInfo.as[RawPanda].filter($"happy".as[Boolean] === true).show()

        //pandasInfo.as[RawPanda].filter(pInfo => pInfo.happy).show()

        //pandasInfo.filter($"attributes" (0) > 0.1).show()



//        pandaPlaces.filter(size($"pandas") > 0 and exists($"pandas", p => p.getField("happy") === lit(true))).show(truncate = false)
//
//        // keep it typed: Dataset[PandaPlace]
//        pandaPlaces
//            .filter(place => place.pandas.nonEmpty && place.pandas.exists(p => p.happy))
//            .show(truncate = false)

//        pandaPlaces.dropDuplicates()
//
//        pandasInfo.select($"id", $"zip", $"pt", $"happy", explode($"attributes").as("attrs")).show(truncate = false)
//
//        Thread.sleep(Int.MaxValue)

//        pandasInfo
//            .groupBy($"zip")
//            .agg {
//                collect_list(struct($"*")).as("pandas")  /*pandasInfo.columns.map(col) */
//            }
//            .show(truncate = false)

        //pandasInfo.sort("id".desc).show(truncate = false)

        //pandasInfo.orderBy($"id".desc).show(truncate = false)

        //val df1 = Seq((1, 2, 3)).toDF("col0", "col1", "col2")/*.withColumns(Map("col11" -> lit(0), "col22" -> lit(0), "col00" -> lit(0)))*/
        //val df2 = Seq((4, 5, 6)).toDF("col11", "col22", "col00")/*.withColumns(Map("col0" -> lit(0), "col1" -> lit(0), "col2" -> lit(0)))*/
        //df1.union(df2).show
        //df1.unionByName(df2).show

        //df1.unionByName(df2, allowMissingColumns = true).show(truncate = false)





        Thread.sleep(Int.MaxValue)

        spark.stop()
