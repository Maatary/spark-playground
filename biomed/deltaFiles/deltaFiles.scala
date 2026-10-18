import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.util.DeltaFileOperations
import scribe.*

import java.time.Instant
import java.util.Locale
import scala.collection.mutable
import scala.util.control.NonFatal

object deltaFiles1:

    private val TableNames  = Vector("relation", "evidence", "node")
    private val MaxAttempts = 3

    private val ExcludedDataDirectories = Set("_delta_log", "_change_data")

    private final case class VersionChange(startVersion: Long, endVersion: Long)

    private final case class TableReport(
        tableName    : String,
        version      : Long,
        activeFiles  : Long,
        inactiveFiles: Long,
        physicalFiles: Long,
        activeBytes  : Long,
        inactiveBytes: Long,
        physicalBytes: Long
    ):
        val inactivePercentage: Double =
            if physicalFiles == 0 then 0.0
            else inactiveFiles.toDouble * 100.0 / physicalFiles.toDouble

    Logger.root
        .clearHandlers()
        .withHandler(minimumLevel = Some(Level.Error))
        .replace()

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("BioMed Delta file inventory")
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

    private def tablePath(basePath: String, tableName: String): String =
        s"${basePath.stripSuffix("/")}/$tableName"

    private def relativeFileKey(fs: FileSystem, tableRoot: Path, filePath: Path): String =
        DeltaFileOperations
            .tryRelativizePath(fs, tableRoot, filePath)
            .toUri
            .getPath
            .stripPrefix("/")

    private def isDataParquet(relativePath: String): Boolean =
        relativePath.toLowerCase(Locale.ROOT).endsWith(".parquet") &&
            !relativePath.split('/').exists(ExcludedDataDirectories.contains)

    private def physicalDataFiles(fs: FileSystem, tableRoot: Path): Map[String, Long] =
        val files    = mutable.Map.empty[String, Long]
        val iterator = fs.listFiles(tableRoot, true)

        while iterator.hasNext do
            val status       = iterator.next()
            val relativePath = relativeFileKey(fs, tableRoot, status.getPath)

            if status.isFile && isDataParquet(relativePath) then
                files += relativePath -> status.getLen

        files.toMap

    private def activeDataFiles(deltaLog: DeltaLog, fs: FileSystem, tableRoot: Path): (Long, Map[String, Long]) =
        val snapshot = deltaLog.update()
        val files    = snapshot.allFiles.collect().iterator.map { addFile =>
            val absolutePath = DeltaFileOperations.absolutePath(deltaLog.dataPath.toString, addFile.path)
            relativeFileKey(fs, tableRoot, absolutePath) -> addFile.size
        }.toMap

        snapshot.version -> files

    private def inspectOnce(
        tableName    : String,
        tableLocation: String,
        deltaLog     : DeltaLog,
        fs           : FileSystem,
        tableRoot    : Path
    ): Either[VersionChange, TableReport] =
        val (startVersion, activeFiles) = activeDataFiles(deltaLog, fs, tableRoot)
        val physicalFiles               = physicalDataFiles(fs, tableRoot)
        val endVersion                  = deltaLog.update().version

        if startVersion != endVersion then Left(VersionChange(startVersion, endVersion))
        else
            val missingActiveFiles = activeFiles.keySet.diff(physicalFiles.keySet)
            require(
              missingActiveFiles.isEmpty,
              s"$tableLocation has ${missingActiveFiles.size} active files missing from the physical S3 listing"
            )

            val sizeMismatches = activeFiles.collect {
                case (path, deltaSize) if physicalFiles(path) != deltaSize =>
                    path -> (deltaSize, physicalFiles(path))
            }
            require(
              sizeMismatches.isEmpty,
              s"$tableLocation has ${sizeMismatches.size} active files whose Delta and S3 sizes differ"
            )

            val inactiveFiles = physicalFiles -- activeFiles.keySet
            val activeBytes   = activeFiles.values.sum
            val inactiveBytes = inactiveFiles.values.sum
            val physicalBytes = physicalFiles.values.sum

            require(physicalFiles.size == activeFiles.size + inactiveFiles.size)
            require(physicalBytes == activeBytes + inactiveBytes)

            Right(
              TableReport(
                tableName     = tableName,
                version       = startVersion,
                activeFiles   = activeFiles.size,
                inactiveFiles = inactiveFiles.size,
                physicalFiles = physicalFiles.size,
                activeBytes   = activeBytes,
                inactiveBytes = inactiveBytes,
                physicalBytes = physicalBytes
              )
            )

    private def inspectTable(spark: SparkSession, tableName: String, tableLocation: String): Either[String, TableReport] =
        val tableRoot = new Path(tableLocation)
        val fs        = tableRoot.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val deltaLog  = DeltaLog.forTable(spark, tableLocation)

        def attempt(number: Int): Either[String, TableReport] =
            try
                inspectOnce(tableName, tableLocation, deltaLog, fs, tableRoot) match
                    case Right(report)                               => Right(report)
                    case Left(versionChange) if number < MaxAttempts => attempt(number + 1)
                    case Left(versionChange)                         =>
                        Left(
                          s"snapshot changed from version ${versionChange.startVersion} " +
                              s"to ${versionChange.endVersion} during all $MaxAttempts attempts"
                        )
            catch
                case NonFatal(error) =>
                    Left(s"${error.getClass.getSimpleName}: ${Option(error.getMessage).getOrElse("no message")}")

        attempt(1)

    private def printReports(basePath: String, outcomes: Vector[(String, Either[String, TableReport])]): Unit =
        println(s"Delta data-file inventory for $basePath")
        println(s"Checked at ${Instant.now()}")
        println()
        println(
          f"${"TABLE"}%-10s ${"VERSION"}%10s ${"ACTIVE"}%10s ${"INACTIVE"}%10s ${"PHYSICAL"}%10s " +
              f"${"ACTIVE_BYTES"}%16s ${"INACTIVE_BYTES"}%16s ${"PHYSICAL_BYTES"}%16s ${"INACTIVE_%"}%11s"
        )

        outcomes.foreach {
            case (_, Right(report))       =>
                println(
                  f"${report.tableName}%-10s ${report.version}%10d ${report.activeFiles}%10d ${report.inactiveFiles}%10d " +
                      f"${report.physicalFiles}%10d ${report.activeBytes}%16d ${report.inactiveBytes}%16d " +
                      f"${report.physicalBytes}%16d ${report.inactivePercentage}%10.2f%%"
                )
            case (tableName, Left(error)) =>
                println(f"$tableName%-10s ERROR: $error")
        }

        println()
        println("Inactive means a physical Parquet data file that is not referenced by the stable current Delta snapshot.")
        println("Transaction-log, change-data, checkpoint, deletion-vector, and directory-marker objects are excluded.")

    private def run(spark: SparkSession, basePath: String): Int =
        val outcomes = TableNames.map { tableName =>
            val location = tablePath(basePath, tableName)
            tableName -> inspectTable(spark, tableName, location)
        }

        printReports(basePath, outcomes)

        if outcomes.exists(_._2.isLeft) then 1 else 0

    def main(args: Array[String]): Unit =
        val basePath = args.toList match
            case path :: Nil => path
            case _           =>
                System.err.println("Usage: deltaFiles1 <delta-graph-base-path>")
                System.err.println(
                  "Example: deltaFiles1 s3a://cmdp-biomed-lakehouse-bucket-uat/silver/biomed-graph/data"
                )
                sys.exit(2)

        val spark = makeSparkSession
        spark.sparkContext.setLogLevel("ERROR")

        val exitCode =
            try run(spark, basePath)
            finally spark.stop()

        if exitCode != 0 then sys.exit(exitCode)
