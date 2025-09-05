
import io.delta.tables.DeltaTable

import scala.compiletime.uninitialized
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StatefulProcessor, TimeMode, TimerValues, ValueState}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.streaming.*
import org.apache.spark.sql.execution.streaming.MemoryStream
import scribe.*
import scribe.format.*

import scala.util.chaining.scalaUtilChainingOps

object Delta1:


    Logger.root
          .clearHandlers()
          .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
          .replace()

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("delta Application")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
            .config("spark.sql.shuffle.partitions", 12)
            .master("local[*]")
            .getOrCreate()


    def main (args: Array[String]): Unit =


        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        spark
            .range(3)
            .coalesce(1)
            .tap{_.explain(true)}
            .write
            .format("delta")
            .mode("overwrite")
            .save("data/delta/delta1")


        val deltaTable = DeltaTable.forPath(spark, "data/delta/delta1")
        deltaTable.delete($"id" === 1)


