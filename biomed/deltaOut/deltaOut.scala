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


import scala.util.chaining.scalaUtilChainingOps

object evidenceOut1:

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
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.sql.shuffle.partitions", 12)
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        val tablePath = "s3a://cmdp-biomed-lakehouse-bucket-int/silver/biomed-graph/data/evidence"

        val evidenceDF = spark.read.delta(tablePath)

//        val withType =
//            evidenceDF
//                .withColumn(
//                    "relation_type",
//                    regexp_extract(col("`biomed:support`"), "^biodata:([^/]+)/", 1)
//                )
//                .withColumn(
//                    "relation_type",
//                    when(length(col("relation_type")) === 0, lit("UNKNOWN"))
//                        .otherwise(col("relation_type"))
//                )
//
//        val perType =
//            withType
//                .groupBy("relation_type")
//                .agg(
//                    count(lit(1)).as("evidence_rows"),
//                    countDistinct(col("`biomed:support`")).as("canonical_relations")
//                )
//                .withColumn(
//                    "avg_evidence_per_canonical",
//                    round(col("evidence_rows").cast("double") / col("canonical_relations"), 2)
//                )
//                .orderBy(desc("evidence_rows"))
//
//        perType.show(200, truncate = false)


//        evidenceDF.agg(
//            count(col("`@id`")).as("id_rows"),
//            countDistinct(col("`@id`")).as("distinct_ids")
//        ).show(false)


        import io.delta.implicits._

        import spark.implicits._
        import org.apache.spark.sql.functions._

        val target = "biodata:binding/partner/Concept_531603644/partner/HGNC11919"

        val evidenceForRelation =
            evidenceDF.filter($"biomed:support" === target)

        println(s"evidence rows: ${evidenceForRelation.count()}")

        evidenceForRelation
            .select(
                $"@id",
                $"biomed:support",
                $"biomed:effect",
                $"biomed:sentenceId",
                $"biomed:sentenceText",
                $"biomed:source",
                $"biomed:doi",
                $"biomed:pmid",
                $"biomed:title"
            )
            .show(200, truncate = false)

        spark.stop()




object evidenceOut2:

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
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.sql.shuffle.partitions", 12)
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        val tablePath = "s3a://cmdp-biomed-lakehouse-bucket-int/silver/biomed-graph/data/evidence"

        val evidenceDF = spark.read.delta(tablePath)

        val withType =
            evidenceDF.withColumn(
                "relation_type",
                regexp_extract(col("`biomed:support`"), "^biodata:([^/]+)/", 1)
            )

        // One row per canonical relation, with how many evidence rows support it
        val supportEvidence =
            withType
                .groupBy(col("relation_type"), col("`biomed:support`").as("support_id"))
                .agg(count(lit(1)).as("evidence_count"))

        // Relation-type level metrics
        val relationSummary =
            supportEvidence
                .groupBy("relation_type")
                .agg(
                    count(lit(1)).as("canonical_relations"), // # unique canonical IDs
                    sum(col("evidence_count")).as("evidence_rows"), // total evidence rows
                    round(avg(col("evidence_count")), 2).as("avg_evidence_per_canonical"),
                    expr("percentile_approx(evidence_count, 0.5)").as("p50_evidence_per_canonical"),
                    expr("percentile_approx(evidence_count, 0.9)").as("p90_evidence_per_canonical"),
                    sum(when(col("evidence_count") === 1, 1).otherwise(0)).as("canonical_with_1_evidence"),
                    sum(when(col("evidence_count") >= 2, 1).otherwise(0)).as("canonical_with_2plus_evidence")
                )
                .orderBy(desc("canonical_relations"))

        relationSummary.show(200, truncate = false)


        spark.stop()




object nodeOut1:

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
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.sql.shuffle.partitions", 12)
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        val tablePath = "s3a://cmdp-biomed-lakehouse-bucket-int/silver/biomed-graph/data/node"

        val nodeDF = spark.read.delta(tablePath)

        nodeDF
            .count()
            .pipe(println)


        spark.stop()
