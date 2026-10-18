import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import io.delta.implicits.*
import scribe.*

import java.time.Instant
import scala.util.control.NonFatal

object deltaCount1:

    private final case class TableCount(
        tablePath: String,
        version  : Long,
        rows     : Long
    )

    Logger.root
        .clearHandlers()
        .withHandler(minimumLevel = Some(Level.Error))
        .replace()

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("BioMed Delta row count")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
              "spark.hadoop.fs.s3a.aws.credentials.provider",
              "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
            )
            .master("local[*]")
            .getOrCreate()

    private def countTable(spark: SparkSession, tablePath: String): Either[String, TableCount] =
        try
            val version = DeltaLog.forTable(spark, tablePath).update().version
            val rows    = spark.read.delta(tablePath).count()
            Right(TableCount(tablePath, version, rows))
        catch
            case NonFatal(error) =>
                Left(s"${error.getClass.getSimpleName}: ${Option(error.getMessage).getOrElse("no message")}")

    private def printReport(outcomes: List[(String, Either[String, TableCount])]): Unit =
        println("Delta table row counts")
        println(s"Checked at ${Instant.now()}")
        println()
        println(f"${"VERSION"}%10s ${"ROWS"}%15s  TABLE")

        outcomes.foreach {
            case (_, Right(count))        =>
                println(f"${count.version}%10d ${count.rows}%,15d  ${count.tablePath}")
            case (tablePath, Left(error)) =>
                println(f"${"-"}%10s ${"-"}%15s  $tablePath ERROR: $error")
        }

    private def run(spark: SparkSession, tablePaths: List[String]): Int =
        val outcomes = tablePaths.map(path => path -> countTable(spark, path))
        printReport(outcomes)
        if outcomes.exists(_._2.isLeft) then 1 else 0

    def main(args: Array[String]): Unit =
        val tablePaths = args.toList match
            case Nil   =>
                System.err.println("Usage: deltaCount1 <delta-table-path> [<delta-table-path> ...]")
                System.err.println(
                  "Example: deltaCount1 s3a://cmdp-biomed-lakehouse-bucket-uat/bronze/biomed-backfill-relation-occurrence-raw-delta"
                )
                sys.exit(2)
            case paths => paths
//"s3://cmdp-biomed-lakehouse-bucket-uat/bronze/biomed-backfill-relation-occurrence-incremental-raw-delta/data/relation-occurrence/"
        val spark = makeSparkSession
        spark.sparkContext.setLogLevel("ERROR")

        val exitCode =
            try run(spark, tablePaths)
            finally spark.stop()

        if exitCode != 0 then sys.exit(exitCode)
