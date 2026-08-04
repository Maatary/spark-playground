import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import fs2.io.file.{Files, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.streaming.*
import scribe.*
import scribe.format.*
import io.delta.implicits.*
import scala.util.chaining.*

object remapperOut1:

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
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.sql.shuffle.partitions", 12)
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        val tablePath = "s3://cmdp-biomed-lakehouse-bucket-int/silver/biomed-backfill-relation-occurrence-termified-delta/data/relation-occurrence/"

        val termifiedOccurrenceDF = spark.read.delta(tablePath)

        termifiedOccurrenceDF
            .count()
            .pipe(println)

//        termifiedOccurrenceDF
//            .tap { _.printSchema() }
//            .show(false)

        spark.stop()

object remapperOut2:

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
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.sql.shuffle.partitions", 12)
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        val tablePath             = "s3://cmdp-biomed-lakehouse-bucket-int/silver/biomed-backfill-relation-occurrence-termified-delta/data/relation-occurrence/"
        val termifiedOccurrenceDF = spark.read.delta(tablePath)

        val occurrenceCountsByCanonicalId =
            termifiedOccurrenceDF
                .groupBy($"canonicalId")
                .agg(countDistinct($"id").as("occurrenceCount"))
                .orderBy(desc("occurrenceCount"), asc("canonicalId"))

        occurrenceCountsByCanonicalId.show(200, truncate = false)

        spark.stop()

object remapperOut3:

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
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.sql.shuffle.partitions", 12)
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        val tablePath             = "s3://cmdp-biomed-lakehouse-bucket-int/silver/biomed-backfill-relation-occurrence-termified-delta/data/relation-occurrence/"
        val termifiedOccurrenceDF = spark.read.delta(tablePath)

        import org.apache.spark.sql.functions._

        val relationAttributeIdNames = Seq(
          "TermiteCellLineId",
          "TermiteCellTypeId",
          "TermiteOrganismId",
          "TermiteOrganId",
          "TermiteTissueId"
        )

        val paraticipantsNodeIds =
            termifiedOccurrenceDF
                .select($"node1.termiteId".as("termiteId"))
                .unionByName(
                  termifiedOccurrenceDF.select($"node2.termiteId".as("termiteId"))
                )
                .select(trim($"termiteId").as("termiteId"))
                .filter($"termiteId".isNotNull && length($"termiteId") > 0)

        val relationAttributeNodeIds =
            termifiedOccurrenceDF
                .select(
                  explode(
                    filter(
                      $"control.attr",
                      a => a.getField("_name").isin(relationAttributeIdNames: _*)
                    )
                  ).as("attr")
                )
                .select(trim($"attr._value").as("termiteId"))
                .filter($"termiteId".isNotNull && length($"termiteId") > 0)

        val trueUniqueNodeCount =
            paraticipantsNodeIds
                .unionByName(relationAttributeNodeIds)
                .distinct()
                .count()

        println(s"trueUniqueNodeCount=$trueUniqueNodeCount")

        // If you also want the actual unique node ID set for inspection:

        val allUniqueNodeIds =
            paraticipantsNodeIds
                .unionByName(relationAttributeNodeIds)
                .distinct()

        allUniqueNodeIds.orderBy($"termiteId").show(200, false)

        spark.stop()

object remapperOut4:

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
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.sql.shuffle.partitions", 12)
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        val tablePath = "s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/silver/node-occurrence-events/all-topics-node-occurrence"
        val allNodes  = spark.read.delta(tablePath)


        allNodes.show(false)

        spark.stop()
