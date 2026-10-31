package biomed3471.codex

import biomed3471.codex.DataTypes.*
import org.apache.hadoop.fs.{Path as HadoopPath}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.util.Using

/** Input and run configuration with sequential Delta persistence for the twin search. */
object IOOps:

    /**
     * Forms the fixed personal DEV destination for one investigation run.
     *
     * @param config the run configuration
     * @return the S3 run prefix with a trailing slash
     */
    def outputPath(config: RunConfig): String = s"$OutputPrefix${config.runId}/"

    /**
     * Forms the output path for one of the six output datasets.
     *
     * Rejects names outside DatasetNames so generated paths follow the six-table layout.
     *
     * @param config the current run
     * @param datasetName a name from DatasetNames
     * @return that dataset's S3 directory
     */
    def datasetPath(config: RunConfig, datasetName: String): String =
        s"${outputPath(config)}$datasetName/"

    /**
     * Creates the local Spark session using the existing Playground S3 setup.
     *
     * Reuses S3A for s3/s3a/s3n and the AWS SDK default credential provider.
     * Uses eight shuffle partitions and all available cores. Other execution settings
     * use Spark and Hadoop defaults. The heap size is supplied by the launch command.
     *
     * @return a local Spark session with the existing Delta integration
     */
    def makeSparkSession: SparkSession =
        SparkSession.builder()
            .appName("BIOMED-3471 Codex")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.sql.shuffle.partitions", 8)
            .master("local[*]")
            .getOrCreate()

    /**
     * Reserves a fresh run prefix and refuses to overwrite any existing run.
     *
     * Writes a small _STARTED marker after checking that the prefix is absent.
     * Partial datasets remain if execution fails. Rerunning the same date
     * requires explicit cleanup of that date's output first.
     *
     * @param spark the session carrying the established Hadoop credentials
     * @param config the new run's destination
     */
    def beginRun(spark: SparkSession, config: RunConfig): Unit =
        val root = new HadoopPath(outputPath(config))
        val fs   = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
        require(!fs.exists(root), s"Output already exists for this date: $root. Choose an unused date or explicitly clean this date's output before rerunning.")
        val body = s"started=${Instant.now()}\ninput=${config.inputPath}\nphase=steps-1-through-5\n"
        Using.resource(fs.create(new HadoopPath(root, "_STARTED"), false)) { stream =>
            stream.write(body.getBytes(StandardCharsets.UTF_8))
        }

    /**
     * Computes a DataFrame and writes it as a new Delta dataset through Spark.
     *
     * The error-if-exists mode prevents replacing prior data. TwinSearchApp calls
     * these writes sequentially.
     *
     * @param data the canonical relations, twin pairs or homonymous node pairs to persist
     * @param config the current run
     * @param datasetName the corresponding agreed dataset name
     */
    def writeDataset(data: DataFrame, config: RunConfig, datasetName: String): Unit =
        val path = datasetPath(config, datasetName)
        println(s"Writing $datasetName to $path")
        data.write.format("delta").mode("errorifexists").save(path)

    /**
     * Writes a dataset and returns a new lazy read of its committed Delta output.
     *
     * Later steps read the saved table, so their writes do not repeat the
     * original grouping or twin join.
     *
     * @param data the dataset to compute and write
     * @param config the current run
     * @param datasetName the agreed output name
     * @return a DataFrame reading that committed output
     */
    def writeAndReadDataset(data: DataFrame, config: RunConfig, datasetName: String): DataFrame =
        writeDataset(data, config, datasetName)
        data.sparkSession.read.format("delta").load(datasetPath(config, datasetName))
