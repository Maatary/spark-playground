import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.io.file.{Files, Path}
import fs2.Stream
import cats.syntax.all.*
import cats.data.Kleisli

import scala.compiletime.uninitialized
import io.delta.tables.DeltaTable
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StatefulProcessor, TimeMode, TimerValues, ValueState}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.streaming.*
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.LongType
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

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import _root_.io.github.pashashiz.spark_encoders.TypedEncoder.given

        spark
            .range(3)
            .coalesce(1)
            .tap { _.explain(true) }
            .write
            .format("delta")
            .mode("overwrite")
            .save("data/delta/delta1")

        val deltaTable = DeltaTable.forPath(spark, "data/delta/delta1")
        deltaTable
            .delete($"id" === 1)

        deltaTable
            .detail()
            .select($"tableFeatures")
            .show(false)

        spark.stop()

object Delta2:

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

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given
        import org.apache.spark.sql.delta.DeltaLog

        val tablePath = "data/delta/delta2"

        Stream
            .eval(Files[IO].deleteRecursively(Path(tablePath)))
            .handleErrorWith { case _: java.nio.file.NoSuchFileException => Stream.unit }
            .compile
            .drain
            .unsafeRunSync()

        spark
            .range(4)
            .coalesce(1)
            .tap { _.explain(true) }
            .write
            .format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .mode("overwrite")
            .save(tablePath)

        val deltaTable = DeltaTable
            .forPath(spark, "data/delta/delta2")
            .tap { _.delete("id = 1") }
            .tap { _.update($"id" % 2 === 0, Map("id" -> lit(8))) }

        val log = DeltaLog.forTable(spark, tablePath)
        log.checkpoint(log.update())

        deltaTable
            .tap {_.update($"id" === 3, Map("id" -> lit(6)))}

        deltaTable
            .detail()
            .select($"tableFeatures")
            .show(false)

        // Read and print the change feed
        spark
            .read
            .format("delta")
            .option("readChangeData", "true")
            .option("startingVersion", "0")
            .load("data/delta/delta2")
            .select(struct($"id").as("entity"), $"_change_type", $"_commit_version", $"_commit_timestamp")
            .withColumn("orderHint", when($"_change_type".isin("delete", "update_preimage"), lit(0)).otherwise(lit(1)))
            .orderBy($"_commit_version".asc, $"orderHint".asc)
            .drop("orderHint")
            .tap { _.printSchema() }
            .tap { _.explain(true) }
            .show(false)

        spark.stop()
