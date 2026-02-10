import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import fs2.io.file.{Files, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import scribe.*
import scribe.format.*

import scala.util.chaining.scalaUtilChainingOps

object DeltaOverwrite1:

    case class User(id: Long, name: String)

    Logger.root
        .clearHandlers()
        .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
        .replace()

    def deleteTableIfExist(tablePath: String): Unit =
        Stream
            .eval(Files[IO].deleteRecursively(Path(tablePath)))
            .handleErrorWith { case _: java.nio.file.NoSuchFileException => Stream.unit }
            .compile
            .drain
            .unsafeRunSync()

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

        val tablePath = "data/delta/deltaoverwrite1"

        deleteTableIfExist(tablePath)

        Seq(
          User(1, "Alice"),
          User(2, "Bob"),
          User(3, "Charlie")
        )
            .toDS
            .coalesce(1)
            .write
            .format("delta")
            .mode(SaveMode.Append)
            .save(tablePath)

        val readDF = spark
            .read
            .format("delta")
            .load(tablePath)
            .tap { _.show(false) }

        readDF
            .write
            .format("delta")
            .mode(SaveMode.Overwrite)
            .save(tablePath)

        spark
            .read
            .format("delta")
            .load(tablePath)
            .tap { _.show(false) }

        spark.stop()
