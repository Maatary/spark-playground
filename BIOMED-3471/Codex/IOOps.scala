package biomed3471.codex

import biomed3471.codex.DataTypes.*
import org.apache.hadoop.fs.{Path as HadoopPath}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant
import scala.util.Using

/** Input and run configuration with sequential Delta persistence for phase one. */
object IOOps:

    /**
     * Parses a unique run name, optional Delta input location and local scratch path.
     *
     * S3 output is fixed to the user's personal DEV investigation prefix.
     * Run names are single path components. The input defaults to the shared
     * S3 Delta source and is retained verbatim for Spark. Scratch defaults to a
     * run-specific local temporary folder.
     * No file, Spark session or S3 object is created while parsing.
     *
     * @param args --run <name> [--input <delta-location>] [--scratch <local-directory>]
     * @return the Delta input location, unique S3 run component and local scratch path
     */
    def parseArguments(args: Array[String]): RunConfig =
        val usage = "Usage: --run <unique-name> [--input <delta-location>] [--scratch <local-directory>]"
        require(args.length % 2 == 0, usage)
        val pairs = args.toList.grouped(2).map(pair => pair.head -> pair(1)).toList
        require(pairs.map(_._1).distinct.size == pairs.size, "Each argument may appear only once.")
        require(pairs.forall { case (key, _) => Set("--run", "--input", "--scratch").contains(key) }, usage)
        val options = pairs.toMap
        val runId   = options.getOrElse("--run", throw new IllegalArgumentException(usage))
        require(runId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,79}"), "Run name must be one safe path component of at most 80 characters.")
        val input   = options.getOrElse("--input", DefaultInputPath)
        val scratch = options.get("--scratch")
            .map(Paths.get(_))
            .getOrElse(Paths.get(System.getProperty("java.io.tmpdir"), "biomed3471-codex", runId))
            .toAbsolutePath.normalize()
        RunConfig(input, runId, scratch)

    /**
     * Forms the fixed personal DEV destination for one investigation run.
     *
     * The validated run component is appended without accepting arbitrary
     * buckets or prefixes from the command line.
     *
     * @param config the parsed run configuration
     * @return the S3 run prefix with a trailing slash
     */
    def outputPath(config: RunConfig): String = s"$OutputPrefix${config.runId}/"

    /**
     * Forms the output path for one of the six agreed phase-one datasets.
     *
     * Rejects unknown dataset names so every write belongs to the documented
     * layout and preserves its intended relation/twin/node-pair unit.
     *
     * @param config the current run
     * @param datasetName a name from DatasetNames
     * @return that dataset's S3 directory
     */
    def datasetPath(config: RunConfig, datasetName: String): String =
        require(DatasetNames.contains(datasetName), s"Unknown phase-one dataset: $datasetName")
        s"${outputPath(config)}$datasetName/"

    /**
     * Creates the local Spark session using the existing Playground S3 setup.
     *
     * Reuses S3A for s3/s3a/s3n and the AWS SDK default credential provider.
     * Local execution uses all available cores. Partitioned shuffles, disabled
     * broadcast joins, disk-backed uploads and spill directories manage memory
     * demand. The JVM heap size is supplied by the launch command.
     *
     * @param config run-specific local scratch directory
     * @return a local Spark session with the existing Delta integration
     */
    def makeSparkSession(config: RunConfig): SparkSession =
        val spill  = Files.createDirectories(config.scratchPath.resolve("spark-spill"))
        val upload = Files.createDirectories(config.scratchPath.resolve("s3-upload"))
        SparkSession.builder()
            .appName(s"BIOMED-3471 phase one ${config.runId}")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.local.dir", spill.toString)
            .config("spark.driver.maxResultSize", "64m")
            .config("spark.memory.offHeap.enabled", "false")
            .config("spark.sql.shuffle.partitions", "256")
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "32m")
            .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
            .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "1")
            .config("spark.hadoop.fs.s3a.buffer.dir", upload.toString)
            .getOrCreate()

    /**
     * Reads the complete exported relation occurrences from the shared Delta source.
     *
     * Retains all exported columns, including node preferred labels and source,
     * without collecting the data.
     * canonicalId restoration and source grouping remain DataOps transformations.
     *
     * @param spark the configured local session
     * @param config the Delta input location
     * @return the lazy DataFrame representing exported relation occurrences
     */
    def readRelationOccurrences(spark: SparkSession, config: RunConfig): DataFrame =
        spark.read.format("delta").load(config.inputPath)

    /**
     * Reserves a fresh run prefix and refuses to overwrite any existing run.
     *
     * Writes a small _STARTED marker after checking that the prefix is absent.
     * Partial datasets remain under that run if execution fails; a later run
     * must use a new name. This does not delete or modify any prior output.
     *
     * @param spark the session carrying the established Hadoop credentials
     * @param config the new run's destination
     */
    def beginRun(spark: SparkSession, config: RunConfig): Unit =
        val root = new HadoopPath(outputPath(config))
        val fs   = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
        require(!fs.exists(root), s"Run prefix already exists; choose a new --run name: $root")
        val body = s"started=${Instant.now()}\ninput=${config.inputPath}\nphase=steps-1-through-5\n"
        Using.resource(fs.create(new HadoopPath(root, "_STARTED"), false)) { stream =>
            stream.write(body.getBytes(StandardCharsets.UTF_8))
        }

    /**
     * Materializes one phase-one DataFrame as a new Delta dataset through Spark.
     *
     * The error-if-exists mode prevents replacing prior data. Writes are called
     * sequentially and use the distributed writer, without collecting to the
     * driver or caching the full investigation. No count action is introduced.
     *
     * @param data the canonical relations, twins, or homonymous node pairs to persist
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
     * This breaks the preceding query lineage so later output writes do not
     * repeat the original grouping or twin join. It does not cache or collect.
     *
     * @param data the dataset to materialize
     * @param config the current run
     * @param datasetName the agreed output name
     * @return a DataFrame reading that committed output
     */
    def writeAndReadDataset(data: DataFrame, config: RunConfig, datasetName: String): DataFrame =
        writeDataset(data, config, datasetName)
        data.sparkSession.read.format("delta").load(datasetPath(config, datasetName))

    /**
     * Records that all six phase-one datasets have been committed when invoked.
     *
     * Records each output path and the sampled JVM peak in a small text marker.
     * _STARTED is retained as the start record. _DATASETS_COMMITTED records
     * successful dataset writes, independently of subsequent Spark shutdown.
     * PhaseOneApp leaves this operation disabled alongside RuntimeOps, so its
     * current execution writes neither this marker nor a local memory report.
     *
     * @param spark the active session
     * @param config the completed run
     * @param sampledPeakResidentBytes the monitor's measured peak so far
     */
    def markDatasetsCommitted(spark: SparkSession, config: RunConfig, sampledPeakResidentBytes: Long): Unit =
        val root = new HadoopPath(outputPath(config))
        val fs   = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val body =
            s"datasetsCommitted=${Instant.now()}\nformat=delta\nsampledPeakAtCommitBytes=$sampledPeakResidentBytes\n" +
            DatasetNames.map(name => s"$name=${datasetPath(config, name)}").mkString("", "\n", "\n")
        Using.resource(fs.create(new HadoopPath(root, "_DATASETS_COMMITTED"), false)) { stream =>
            stream.write(body.getBytes(StandardCharsets.UTF_8))
        }
