import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.io.file.{Files, Path}
import fs2.Stream
import cats.syntax.all.*

import scala.compiletime.uninitialized

import io.github.pashashiz.spark_encoders.TypedEncoder
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}
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
        import _root_.io.github.pashashiz.spark_encoders.TypedEncoder.given

        val tablePath = "data/delta/delta1"

        deleteTableIfExist(tablePath)

        spark
            .range(3)
            .coalesce(1)
            .tap { _.explain(true) }
            .write
            .format("delta")
            .mode("overwrite")
            .save(tablePath)

        val deltaTable = DeltaTable.forPath(spark, tablePath)
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
        import org.apache.spark.sql.delta.DeltaLog

        val tablePath = "data/delta/delta2"

        deleteTableIfExist(tablePath)

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
            .forPath(spark, tablePath)
            .tap { _.delete("id = 1") }
            .tap {
                _.update($"id" % 2 === 0, Map("id" -> lit(8), "name" -> lit("Updated")))
            }

        // Introduced solely to look into the checkpoint quickly
        // Delta checkpoint automatically when the checkpoint interval is reached default is 10 commit
        val log = DeltaLog.forTable(spark, tablePath)
        log.checkpoint(log.update())

        deltaTable
            .tap {_.update($"id" === 3, Map("id" -> lit(6), "name" -> lit("Modified")))}

        // table features
        deltaTable
            .detail()
            .select($"tableFeatures")
            .show(false)

        // Read and print the change feed
        spark
            .read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "0") // <- In batch mode, we need to specify the starting version
            .load("data/delta/delta2")
            .select(struct($"id").as("entity"), $"_change_type", $"_commit_version", $"_commit_timestamp")
            .withColumn("orderHint", when($"_change_type".isin("delete", "update_preimage"), lit(0)).otherwise(lit(1)))
            .orderBy($"_commit_version".asc, $"orderHint".asc)
            .drop("orderHint")
            .tap { _.printSchema() }
            .tap { _.explain(true) }
            .show(false)

        val cp = spark
            .read
            .parquet("data/delta/delta2/_delta_log/00000000000000000002.checkpoint.parquet")
            .tap { _.printSchema() }
            .tap { _.show(true) }
            .write
            .json("data/delta/delta2/_delta_log/00000000000000000002.checkpoint.json")

        spark.stop()


object Delta3:


    case class User(id: Long, name: String)
    case class RawChange[T](entity: T, changeType: String, commitVersion: Long, commitTimestamp: java.sql.Timestamp)

    Logger.root
        .clearHandlers()
        .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
        .replace()


    def createDeltaTableFor[T](spark: SparkSession, path: String, enableCDF: Boolean = true)(using encT: Encoder[T]): Unit = {
        import org.apache.spark.sql.types._

        // Build SQL type strings for all Spark data types (works for nested types too) including constraints e.g., NOT NULL
        def sqlType(dt: DataType): String = dt.sql // e.g., BIGINT, STRING, STRUCT<...>, ARRAY<...>

        val b0 = DeltaTable.create(spark).location(path) // <-- must be an absolute path otherwise interpreted as a table name
        val b1 = encT.schema.fields.foldLeft(b0) { (b, f) =>
            b.addColumn(f.name, sqlType(f.dataType), f.nullable)
        }

        val b2 = if (enableCDF) then b1.property("delta.enableChangeDataFeed", "true") else b1
        b2.execute()
    }

    def entityStructFor[T](name: String = "entity")(using enc: Encoder[T]): Column =
        struct(enc.schema.fieldNames.map(col)*).as(name)

    def createUnsafeRawChangeDSFor[T](cdfDF: DataFrame)(using entityEnc: Encoder[T], rawChangeEnc: Encoder[RawChange[T]]): Dataset[RawChange[T]] = {
        cdfDF.select(
          entityStructFor[T]("entity"),
          col("_change_type").as("changeType"),
          col("_commit_version").as("commitVersion"),
          col("_commit_timestamp").as("commitTimestamp")
        )
        .as[RawChange[T]]
    }

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
        import org.apache.spark.sql.delta.DeltaLog

        val tablePath = "data/delta/delta3"

        deleteTableIfExist(tablePath)

        //commit 0
        createDeltaTableFor[User](spark, Path(tablePath).absolute.toString, true)

        //commit 1
        Seq(User(1, "Alice"), User(2, "Bob"), User(3, "Charlie"), User(4, "Dave"))
            .toDS
            .coalesce(1)
            .tap { _.printSchema() }
            .tap { _.explain(true) }
            .write
            .format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .mode("overwrite")
            .save(tablePath)

        //commit 2
        val deltaTable = DeltaTable
            .forPath(spark, tablePath)
            .tap { _.delete("id = 1") }
            .tap { _.update($"id" % 2 === 0, Map("name" -> concat(lit("Dr. "), col("name")))) }

        //commit 3
        deltaTable
            .tap { _.update($"id" === 3, Map("name" -> concat(lit("Mr. "), col("name")))) }

        val log = DeltaLog.forTable(spark, tablePath)
        log.checkpoint(log.update())

        // table features
        deltaTable
            .detail()
            .select($"tableFeatures")
            .show(false)

        // Read the change feed
        val cdfDF = spark
            .read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "0") // <- In batch mode, we need to specify the starting version
            .load(tablePath)
            .tap { _.printSchema() }

        println("==== Encoder schema for RawChange[User] ====")
        TypedEncoder[RawChange[User]].encoder.schema.printTreeString()
        TypedEncoder[RawChange[User]].encoder.encoder.schema.printTreeString()
        println("============================================")


        val rawChangesDS = createUnsafeRawChangeDSFor[User](cdfDF) //<-- Exploring the pattern
            //.select(entityStructFor[User]("entity"), $"_change_type", $"_commit_version", $"_commit_timestamp")
            .withColumn("orderHint", when($"changeType".isin("delete", "update_preimage"), lit(0)).otherwise(lit(1)))
            .orderBy($"commitVersion".asc, $"orderHint".asc)
            .drop("orderHint")
            .tap { _.printSchema() }
            .tap { _.explain(true) }
            .show(false)

        // 04 checkpoint because we did 4 commits with table creation being commit 0
        val cp = spark
            .read
            .parquet(s"$tablePath/_delta_log/00000000000000000004.checkpoint.parquet")
            .tap { _.printSchema() }
            .tap { _.show(true) }
            .write
            .json(s"$tablePath/_delta_log/00000000000000000004.checkpoint.json")

        spark.stop()


object Delta4:


    case class User(id: Long, name: String)
    case class RawChange[T](entity: T, changeType: String, commitVersion: Long, commitTimestamp: java.sql.Timestamp)

    Logger.root
        .clearHandlers()
        .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
        .replace()


    def createCdfDF(spark: SparkSession, tablePath: String): DataFrame =
        spark
            .read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "0") // <- In batch mode, we need to specify the starting version
            .load(tablePath)

    def createDeltaTableFor[T](spark: SparkSession, path: String, enableCDF: Boolean = true)(using encT: Encoder[T]): Unit = {
        import org.apache.spark.sql.types._

        // Build SQL type strings for all Spark data types (works for nested types too) including constraints e.g., NOT NULL
        def sqlType(dt: DataType): String = dt.sql // e.g., BIGINT, STRING, STRUCT<...>, ARRAY<...>

        val b0 = DeltaTable.create(spark).location(path) // <-- must be an absolute path otherwise interpreted as a table name
        val b1 = encT.schema.fields.foldLeft(b0) { (b, f) =>
            b.addColumn(f.name, sqlType(f.dataType), f.nullable)
        }

        val b2 = if enableCDF then b1.property("delta.enableChangeDataFeed", "true") else b1
        b2.execute()
    }

    def entityStructFor[T](name: String = "entity")(using enc: Encoder[T]): Column =
        struct(enc.schema.fieldNames.map(col)*).as(name)

    def createUnsafeRawChangeDSFor[T](cdfDF: DataFrame)(using entityEnc: Encoder[T], rawChangeEnc: Encoder[RawChange[T]]): Dataset[RawChange[T]] = {
        cdfDF.select(
          entityStructFor[T]("entity"),
          col("_change_type").as("changeType"),
          col("_commit_version").as("commitVersion"),
          col("_commit_timestamp").as("commitTimestamp")
        )
        .as[RawChange[T]]
    }

    def computeSortedRawChangeDSFor[T](rawDS: Dataset[RawChange[T]])(using rawChangeEnc: Encoder[RawChange[T]]): Dataset[RawChange[T]] =
        rawDS
            .withColumn("orderHint", when(col("changeType").isin("delete", "update_preimage"), lit(0)).otherwise(lit(1)))
            .orderBy(col("commitVersion").asc, col("orderHint").asc)
            .drop("orderHint")
            .as[RawChange[T]]

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
        import org.apache.spark.sql.delta.DeltaLog

        val tablePath = "data/delta/delta4"

        deleteTableIfExist(tablePath)

        //commit 0
        createDeltaTableFor[User](spark, Path(tablePath).absolute.toString, true)

        //commit 1
        Seq(User(1, "Alice"), User(2, "Bob"), User(3, "Charlie"), User(4, "Dave"))
            .toDS
            .coalesce(1)
            .tap { _.printSchema() }
            .tap { _.explain(true) }
            .write
            .format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .mode("overwrite")
            .save(tablePath)


        val deltaTable = DeltaTable.forPath(spark, tablePath)

        //commit 2
        deltaTable.delete($"id" === 1)

        //commit 3
        deltaTable
            .as("target")
            .merge(
                Seq(User(2, "Dr. Bob"),
                    User(4, "Dr. Dave"),
                    User(5, "Dr. Davis max")).toDF.as("source"),
                $"target.id" === $"source.id")
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .execute()

        //commit 4
        deltaTable
            .as("target")
            .merge(
                Seq(User(3, "Mr. Charlie")).toDF.as("source"),
                $"target.id" === $"source.id"
            )
            .whenMatched()
            .updateAll()
            .execute()


        createCdfDF(spark, tablePath)
            .tap  { _.printSchema() }
            .pipe { cdfDF => createUnsafeRawChangeDSFor[User](cdfDF) }
            .pipe { rawChangesDS => computeSortedRawChangeDSFor[User](rawChangesDS) }
            .tap  { _.printSchema() }
            .tap  { _.show(false) }


        spark.stop()


/**
 * = Moving to Streaming =
 *
 * == __Note__: ==
 *
 * As per the delta table specification, a commit can contain at most one change per record. Multiple change to the same
 * record is inconsistent and can not happen. Merge semantic block multiple matching record in the source DF.
 *
 * To reliably test a scenario where a micro batch contains multiple changes to the same record in a controlled fashion,
 * we need to predictably read the source delta table such that we can be sure that the two commits that contains the two changes
 * are red as part of one micro-batch.
 * we need to control the number of files belonging to a commit. If we can get 1 file per commit, then we can reliably read
 * as per the experiment specification.
 *
 * With AQE the partition created by the suffled induced by the delta merge operation is collapse to 1
 * given the small amount of data that goes into theses test.
 *
 * This ensure that we have one commit per file, provided that we first create a delta table with one file only.
 *
 * maxFilesPerTrigger = 2
 *
 */
object Delta5:


    case class User(id: Long, name: String)
    case class RawChange[T](entity: T, changeType: String, commitVersion: Long, commitTimestamp: java.sql.Timestamp)

    Logger.root
        .clearHandlers()
        .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
        .replace()

    def createCdfDF(spark: SparkSession, tablePath: String, maxFilesPerTrigger: Int ): DataFrame =
        spark
            .readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "0")
            .option("maxFilesPerTrigger", maxFilesPerTrigger)
            .load(tablePath)

    def createDeltaTableFor[T](spark: SparkSession, path: String, enableCDF: Boolean = true)(using encT: Encoder[T]): Unit = {
        import org.apache.spark.sql.types._

        // Build SQL type strings for all Spark data types (works for nested types too) including constraints e.g., NOT NULL
        def sqlType(dt: DataType): String = dt.sql // e.g., BIGINT, STRING, STRUCT<...>, ARRAY<...>

        val b0 = DeltaTable.create(spark).location(path) // <-- must be an absolute path otherwise interpreted as a table name
        val b1 = encT.schema.fields.foldLeft(b0) { (b, f) =>
            b.addColumn(f.name, sqlType(f.dataType), f.nullable)
        }

        val b2 = if enableCDF then b1.property("delta.enableChangeDataFeed", "true") else b1
        b2.execute()
    }

    def entityStructFor[T](name: String = "entity")(using enc: Encoder[T]): Column =
        struct(enc.schema.fieldNames.map(col)*).as(name)

    def createUnsafeRawChangeDSFor[T](cdfDF: DataFrame)(using entityEnc: Encoder[T], rawChangeEnc: Encoder[RawChange[T]]): Dataset[RawChange[T]] = {
        cdfDF.select(
          entityStructFor[T]("entity"),
          col("_change_type").as("changeType"),
          col("_commit_version").as("commitVersion"),
          col("_commit_timestamp").as("commitTimestamp")
        )
        .as[RawChange[T]]
    }

    def computeSortedRawChangeDSFor[T](rawDS: Dataset[RawChange[T]])(using rawChangeEnc: Encoder[RawChange[T]]): Dataset[RawChange[T]] =
        rawDS
            .withColumn("orderHint", when(col("changeType").isin("delete", "update_preimage"), lit(0)).otherwise(lit(1)))
            .orderBy(col("commitVersion").asc, col("orderHint").asc)
            .drop("orderHint")
            .as[RawChange[T]]

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
        import org.apache.spark.sql.delta.DeltaLog

        val tablePath = "data/delta/delta5"

        deleteTableIfExist(tablePath)

        //commit 0
        createDeltaTableFor[User](spark, Path(tablePath).absolute.toString, true)

        //commit 1
        Seq(User(1, "Alice"), User(2, "Bob"), User(3, "Charlie"), User(4, "Dave"))
            .toDS
            .coalesce(1)
            .tap { _.printSchema() }
            .tap { _.explain(true) }
            .write
            .format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .mode("overwrite")
            .save(tablePath)


        val deltaTable = DeltaTable.forPath(spark, tablePath)

        //commit 2
        deltaTable.delete($"id" === 1)

        //commit 3
        deltaTable
            .as("target")
            .merge(
                Seq(User(2, "Dr. Bob"),
                    User(4, "Dr. Dave"),
                    User(5, "Dr. Davis")).toDF.as("source"),
                $"target.id" === $"source.id")
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .execute()

        //commit 4
        deltaTable
            .as("target")
            .merge(
                Seq(User(3, "Mr. Charlie"),
                    User(4, "Super Dr. Dave")).toDF.as("source"),
                $"target.id" === $"source.id"
            )
            .whenMatched()
            .updateAll()
            .execute()


        createCdfDF(spark, tablePath, maxFilesPerTrigger = 2)
            .tap  { _.printSchema() }
            .pipe { cdfDF => createUnsafeRawChangeDSFor[User](cdfDF) }
            .tap  { _.printSchema() }
            .writeStream
            .foreachBatch { (ds:Dataset[RawChange[User]], l:Long) =>
                computeSortedRawChangeDSFor[User](ds)
                    .show(false)
            }
            .trigger(Trigger.AvailableNow)
            .start()
            .awaitTermination()

        spark.stop()


object Delta6:


    case class User(id: Long, name: String)
    case class RawChange[T](entity: T, changeType: String, commitVersion: Long, commitTimestamp: java.sql.Timestamp)

    Logger.root
        .clearHandlers()
        .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
        .replace()

    def runShowRawChangeDSFor[T](rawChangesDS: Dataset[RawChange[T]], trigger: Trigger)(using rawEnc: Encoder[RawChange[T]]): StreamingQuery =
        rawChangesDS
            .writeStream
            .foreachBatch { (ds: Dataset[RawChange[T]], batchId: Long) =>
                computeSortedRawChangeDSFor[T](ds).show(false)
            }
            .trigger(trigger)
            .start()


    def UpsertUserDSIntoDelta(deltaTable: DeltaTable, sourceDS: Dataset[User]): Unit =
        deltaTable
            .as("target")
            .merge(sourceDS.toDF.as("source"),
                col("target.id") === col("source.id")
            )
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .execute()

    def createSampleDSFor[T](seq: Seq[T], partition: Int)(using spark: SparkSession, enc: Encoder[T]): Dataset[T] =
        import spark.implicits.localSeqToDatasetHolder
        seq
            .toDS
            .coalesce(partition)
            .tap { _.printSchema() }
            .tap { _.explain(true) }

    def writeDStoDeltaTable[T](ds: Dataset[T], path: String): Unit =
        ds
            .write
            .format("delta")
            .mode("append")
            .save(path)

    def createCdfDF(spark: SparkSession, tablePath: String, maxFilesPerTrigger: Int ): DataFrame =
        spark
            .readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "0")
            .option("maxFilesPerTrigger", maxFilesPerTrigger)
            .load(tablePath)
            .tap { _.printSchema() }

    def createDeltaTableFor[T](spark: SparkSession, path: String, enableCDF: Boolean = true)(using encT: Encoder[T]): Unit = {
        import org.apache.spark.sql.types._

        // Build SQL type strings for all Spark data types (works for nested types too) including constraints e.g., NOT NULL
        def sqlType(dt: DataType): String = dt.sql // e.g., BIGINT, STRING, STRUCT<...>, ARRAY<...>

        val b0 = DeltaTable.create(spark).location(path) // <-- must be an absolute path otherwise interpreted as a table name
        val b1 = encT.schema.fields.foldLeft(b0) { (b, f) =>
            b.addColumn(f.name, sqlType(f.dataType), f.nullable)
        }

        val b2 = if enableCDF then b1.property("delta.enableChangeDataFeed", "true") else b1
        b2.execute()
    }

    def entityStructFor[T](name: String = "entity")(using enc: Encoder[T]): Column =
        struct(enc.schema.fieldNames.map(col)*).as(name)

    def createUnsafeRawChangeDSFor[T](cdfDF: DataFrame)(using entityEnc: Encoder[T], rawChangeEnc: Encoder[RawChange[T]]): Dataset[RawChange[T]] = {
        cdfDF.select(
          entityStructFor[T]("entity"),
          col("_change_type").as("changeType"),
          col("_commit_version").as("commitVersion"),
          col("_commit_timestamp").as("commitTimestamp")
        )
        .as[RawChange[T]]
        .tap { _.printSchema() }

    }

    def computeSortedRawChangeDSFor[T](rawDS: Dataset[RawChange[T]])(using rawChangeEnc: Encoder[RawChange[T]]): Dataset[RawChange[T]] =
        rawDS
            .withColumn("orderHint", when(col("changeType").isin("delete", "update_preimage"), lit(0)).otherwise(lit(1)))
            .orderBy(col("commitVersion").asc, col("orderHint").asc)
            .drop("orderHint")
            .as[RawChange[T]]

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

        implicit val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        val tablePath = "data/delta/delta6"
        val users     = Seq(User(1, "Alice"), User(2, "Bob"), User(3, "Charlie"), User(4, "Dave"))


        deleteTableIfExist(tablePath)

        //commit 0
        createDeltaTableFor[User](spark, Path(tablePath).absolute.toString, true)

        val deltaTable = DeltaTable.forPath(spark, tablePath)


        //commit 1
        createSampleDSFor[User](users, partition = 1)
            .pipe { ds => writeDStoDeltaTable(ds, tablePath) }

        //commit 2
        deltaTable.delete($"id" === 1)

        //commit 3
        val userUpdate1 = Seq(
            User(2, "Dr. Bob"),
            User(4, "Dr. Dave"),
            User(5, "Dr. Davis")
        )
        UpsertUserDSIntoDelta(deltaTable, userUpdate1.toDS)

        //commit 4
        val userUpdate2 = Seq(
            User(3, "Mr. Charlie"),
            User(4, "Super Dr. Dave")
        )
        UpsertUserDSIntoDelta(deltaTable, userUpdate2.toDS)


        val userRawChangeDS = createCdfDF(spark, tablePath, maxFilesPerTrigger = 2)
            .pipe { cdfDF => createUnsafeRawChangeDSFor[User](cdfDF) }

        val rawChangeQuery = runShowRawChangeDSFor[User](userRawChangeDS, Trigger.AvailableNow)

        rawChangeQuery.awaitTermination()

        spark.stop()



object Delta7:


    case class User(id: Long, name: Option[String])
    case class RawChange[T](entity: T, changeType: String, commitVersion: Long, commitTimestamp: java.sql.Timestamp)

    Logger.root
        .clearHandlers()
        .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
        .replace()

    def runShowRawChangeDSFor[T](rawChangesDS: Dataset[RawChange[T]], trigger: Trigger)(using rawEnc: Encoder[RawChange[T]]): StreamingQuery =
        rawChangesDS
            .writeStream
            .foreachBatch { (ds: Dataset[RawChange[T]], batchId: Long) =>
                computeSortedRawChangeDSFor[T](ds).show(false)
            }
            .trigger(trigger)
            .start()


    def runUpsertUserDSIntoDelta(deltaTable: DeltaTable, userDS: Dataset[User]): StreamingQuery = {

        val allOtherNull = userDS
          .columns
          .filterNot(_ == "id")
          .map(colName => col("source." + colName).isNull)
          .reduce(_ && _)

        userDS
          .writeStream
          .foreachBatch { (ds: Dataset[User], batchId: Long) =>
              deltaTable
                .as("target")
                .merge(
                  ds.toDF.as("source"),
                  col("target.id") === col("source.id")
                )
                .whenMatched(allOtherNull)
                .delete()
                .whenMatched()
                .updateAll()
                .whenNotMatched(!allOtherNull) // <-- We don't insert tombstone.
                .insertAll()
                .execute()
                .pipe { _ => () }
          }
          .start()
    }

    def createDSFromMemSrcFor[T](memSrc: MemoryStream[T], partition: Int): Dataset[T] =
        memSrc.toDS()

    def createCdfDF(spark: SparkSession, tablePath: String, maxFilesPerTrigger: Int ): DataFrame =
        spark
            .readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "0")
            .option("maxFilesPerTrigger", maxFilesPerTrigger)
            .load(tablePath)
            .tap { _.printSchema() }

    def createDeltaTableFor[T](spark: SparkSession, path: String, enableCDF: Boolean = true)(using encT: Encoder[T]): Unit = {
        import org.apache.spark.sql.types._

        // Build SQL type strings for all Spark data types (works for nested types too) including constraints e.g., NOT NULL
        def sqlType(dt: DataType): String = dt.sql // e.g., BIGINT, STRING, STRUCT<...>, ARRAY<...>

        val b0 = DeltaTable.create(spark).location(path) // <-- must be an absolute path otherwise interpreted as a table name
        val b1 = encT.schema.fields.foldLeft(b0) { (b, f) =>
            b.addColumn(f.name, sqlType(f.dataType), f.nullable)
        }

        val b2 = if enableCDF then b1.property("delta.enableChangeDataFeed", "true") else b1
        b2.execute()
    }

    def entityStructFor[T](name: String = "entity")(using enc: Encoder[T]): Column =
        struct(enc.schema.fieldNames.map(col)*).as(name)

    def createUnsafeRawChangeDSFor[T](cdfDF: DataFrame)(using entityEnc: Encoder[T], rawChangeEnc: Encoder[RawChange[T]]): Dataset[RawChange[T]] = {
        cdfDF.select(
          entityStructFor[T]("entity"),
          col("_change_type").as("changeType"),
          col("_commit_version").as("commitVersion"),
          col("_commit_timestamp").as("commitTimestamp")
        )
        .as[RawChange[T]]
        .tap { _.printSchema() }

    }

    def computeSortedRawChangeDSFor[T](rawDS: Dataset[RawChange[T]])(using rawChangeEnc: Encoder[RawChange[T]]): Dataset[RawChange[T]] =
        rawDS
            .withColumn("orderHint", when(col("changeType").isin("delete", "update_preimage"), lit(0)).otherwise(lit(1)))
            .orderBy(col("commitVersion").asc, col("orderHint").asc)
            .drop("orderHint")
            .as[RawChange[T]]

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

        implicit val spark  = makeSparkSession
        implicit val sqlCtx = spark.sqlContext

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given



        val tablePath = "data/delta/delta7"


        deleteTableIfExist(tablePath)

        //commit 0
        createDeltaTableFor[User](spark, Path(tablePath).absolute.toString, true)

        val deltaTable  = DeltaTable.forPath(spark, tablePath)
        val memSrc      = MemoryStream[User]
        val userDS      = createDSFromMemSrcFor[User](memSrc, partition = 1)

        val userDSInToDeltaQuery = runUpsertUserDSIntoDelta(deltaTable, userDS)

        //commit 1
        memSrc.addData(Seq(
            User(1, Some("Alice")),
            User(2, Some("Bob")),
            User(3, Some("Charlie")),
            User(4, Some("Dave"))
        ))
        userDSInToDeltaQuery.processAllAvailable()

        //commit 2
        memSrc.addData(Seq(
            User(2, Some("Dr. Bob")),
            User(4, Some("Dr. Dave")),
            User(5, Some("Dr. Davis"))
        ))
        userDSInToDeltaQuery.processAllAvailable()


        //commit 3
         memSrc.addData(Seq(
             User(3, Some("Mr. Charlie")),
             User(4, Some("Super Dr. Dave"))
         ))
        userDSInToDeltaQuery.processAllAvailable()


        //commit 4
        memSrc.addData(Seq(
            User(5, None),
            User(3, Some("Super Mr. Charlie")),
        ))
        userDSInToDeltaQuery.processAllAvailable()




        val userRawChangeDS = createCdfDF(spark, tablePath, maxFilesPerTrigger = 2)
            .pipe { cdfDF => createUnsafeRawChangeDSFor[User](cdfDF) }

        val rawChangeQuery = runShowRawChangeDSFor[User](userRawChangeDS, Trigger.AvailableNow)

        rawChangeQuery.awaitTermination()

        spark.stop()



object Delta8:

    Logger.root
          .clearHandlers()
          .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
          .replace()



    case class User(id: String, name: Option[String])
    case class RawChange[T](entity: T, changeType: String, commitVersion: Long, commitTimestamp: java.sql.Timestamp)

    sealed abstract class RowChange[+T](val entity: T, val version: Long)
    case class Add[T](override val entity: T, override val version: Long) extends RowChange[T](entity, version)
    case class Remove[T](override val entity: T, override val version: Long) extends RowChange[T](entity, version)

    sealed abstract class LogChange[+T](val key: String, val offset: Option[Long])
    case class Upsert[T](override val key: String, val entity: T, override val offset: Option[Long] = none) extends LogChange[T](key, offset)
    case class Tombstone[T](override val key: String, override val offset: Option[Long] = none) extends LogChange[T](key, offset)







    def computeSortedUserChangeDS(changes: Dataset[RowChange[User]])(using Encoder[RowChange[User]]): Dataset[RowChange[User]] =
        changes
          .toDF
          .withColumn("orderHint", when(col("_type") === lit("Remove"), lit(0)).otherwise(lit(1)))
          .orderBy(col("version").asc, col("orderHint").asc)
          .drop("orderHint")
          .as[RowChange[User]]

    def runShowUserChangeDS(userChangeDS: Dataset[RowChange[User]], trigger: Trigger)(using Encoder[RowChange[User]]): StreamingQuery =
        userChangeDS
          .writeStream
          .foreachBatch { (ds: Dataset[RowChange[User]], _: Long) =>
              computeSortedUserChangeDS(ds).show(false)
          }
          .trigger(trigger)
          .start()

    def createUnsafeMergeSrcDFForChangeLog[T](ds: Dataset[LogChange[T]], idColName: String = "id")(using encT: Encoder[T]): DataFrame =
        val nonKeyCols = encT.schema.fieldNames.filterNot(_ == idColName).map(f => col(s"entity.`$f`").as(f))
        ds.toDF
          .select((col("key").as(idColName) +: nonKeyCols :+ col("_type")): _*)
          .alias("source")


    def runUnsafeUpsertChangeLogIntoDeltaFor[T](deltaTable: DeltaTable, ds: Dataset[LogChange[T]], idColName: String)(using Encoder[T]): StreamingQuery =
        ds
          .writeStream
          .foreachBatch { (ds: Dataset[LogChange[T]], _: Long) =>

              val source = createUnsafeMergeSrcDFForChangeLog[T](ds, idColName) // id, non-keys..., _type

              deltaTable
                .as("target")
                .merge(
                    source.as("source"),
                    col(s"target.`$idColName`") === col(s"source.`$idColName`")
                )
                .whenMatched(col("source._type") === lit("Tombstone")).delete()
                .whenMatched(col("source._type") === lit("Upsert")).updateAll()
                .whenNotMatched(col("source._type") === lit("Upsert")).insertAll()
                .execute()
                .pipe { _ => () }
          }
          .start()


    def createDSFromMemSrcFor[T](memSrc: MemoryStream[T], partition: Int): Dataset[T] =
        memSrc.toDS()

    def createCdfDF(spark: SparkSession, tablePath: String, maxFilesPerTrigger: Int ): DataFrame =
        spark
            .readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "0")
            .option("maxFilesPerTrigger", maxFilesPerTrigger)
            .load(tablePath)
            .tap { _.printSchema() }

    def createDeltaTableFor[T](spark: SparkSession, path: String, enableCDF: Boolean = true)(using encT: Encoder[T]): Unit = {
        import org.apache.spark.sql.types._

        // Build SQL type strings for all Spark data types (works for nested types too) including constraints e.g., NOT NULL
        def sqlType(dt: DataType): String = dt.sql // e.g., BIGINT, STRING, STRUCT<...>, ARRAY<...>

        val b0 = DeltaTable.create(spark).location(path) // <-- must be an absolute path otherwise interpreted as a table name
        val b1 = encT.schema.fields.foldLeft(b0) { (b, f) =>
            b.addColumn(f.name, sqlType(f.dataType), f.nullable)
        }

        val b2 = if enableCDF then b1.property("delta.enableChangeDataFeed", "true") else b1
        b2.execute()
    }

    def entityStructFor[T](name: String = "entity")(using enc: Encoder[T]): Column =
        struct(enc.schema.fieldNames.map(col)*).as(name)

    def createUnsafeRawChangeDSFor[T](cdfDF: DataFrame)(using entityEnc: Encoder[T], rawChangeEnc: Encoder[RawChange[T]]): Dataset[RawChange[T]] = {
        cdfDF.select(
          entityStructFor[T]("entity"),
          col("_change_type").as("changeType"),
          col("_commit_version").as("commitVersion"),
          col("_commit_timestamp").as("commitTimestamp")
        )
        .as[RawChange[T]]
        .tap { _.printSchema() }

    }

    def computeSortedRawChangeDSFor[T](rawDS: Dataset[RawChange[T]])(using rawChangeEnc: Encoder[RawChange[T]]): Dataset[RawChange[T]] =
        rawDS
            .withColumn("orderHint", when(col("changeType").isin("delete", "update_preimage"), lit(0)).otherwise(lit(1)))
            .orderBy(col("commitVersion").asc, col("orderHint").asc)
            .drop("orderHint")
            .as[RawChange[T]]

    def computeChangeDSFor[T](raw: Dataset[RawChange[T]])(using Encoder[RowChange[T]]): Dataset[RowChange[T]] =
        raw.map { rc =>
            rc.changeType match
                case "insert" | "update_postimage" => Add(rc.entity, rc.commitVersion)
                case "delete" | "update_preimage"  => Remove(rc.entity, rc.commitVersion)
        }

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

        implicit val spark  = makeSparkSession
        implicit val sqlCtx = spark.sqlContext

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given



        val tablePath = "data/delta/delta8"


        deleteTableIfExist(tablePath)

        //commit 0
        createDeltaTableFor[User](spark, Path(tablePath).absolute.toString, true)

        val deltaTable      = DeltaTable.forPath(spark, tablePath)
        val memSrc          = MemoryStream[LogChange[User]]
        val userLogChangeDS = createDSFromMemSrcFor[LogChange[User]](memSrc, partition = 1)

        val upsertUserQuery = runUnsafeUpsertChangeLogIntoDeltaFor[User](deltaTable, userLogChangeDS, idColName = "id")

        //commit 1
        memSrc.addData(Seq(
            Upsert("1", User("1", Some("Alice"))),
            Upsert("2", User("2", Some("Bob"))),
            Upsert("3", User("3", Some("Charlie"))),
            Upsert("4", User("4", Some("Dave")))
        ))
        upsertUserQuery.processAllAvailable()

        //commit 2
        memSrc.addData(Seq(
            Upsert("2", User("2", Some("Dr. Bob"))),
            Upsert("4", User("4", Some("Dr. Dave"))),
            Upsert("5", User("5", Some("Dr. Davis")))
        ))
        upsertUserQuery.processAllAvailable()


        //commit 3
         memSrc.addData(Seq(
             Upsert("3", User("3", Some("Mr. Charlie"))),
             Upsert("4", User("4", Some("Super Dr. Dave")))
         ))
        upsertUserQuery.processAllAvailable()


        //commit 4
        memSrc.addData(Seq(
            Tombstone[User]("5"),
            Upsert("3", User("3", Some("Super Mr. Charlie"))),
        ))
        upsertUserQuery.processAllAvailable()




        val userChangeDS: Dataset[RowChange[User]] = createCdfDF(spark, tablePath, maxFilesPerTrigger = 2)
          .pipe { cdfDF       => createUnsafeRawChangeDSFor[User](cdfDF) }
          .pipe { rowChangeDS => computeChangeDSFor[User](rowChangeDS)}

        val showUserQuery = runShowUserChangeDS(userChangeDS, Trigger.AvailableNow)

        showUserQuery.awaitTermination()


        spark
          .read
          .format("delta")
          .load(tablePath)
          .tap { _.printSchema() }
          .tap { _.show() }

        spark.stop()
