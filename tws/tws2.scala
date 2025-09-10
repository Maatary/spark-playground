package tws2

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.io.file.{Files, Path}
import fs2.Stream
import scala.compiletime.uninitialized

import org.apache.spark.sql.*
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.streaming.*
import org.apache.spark.sql.execution.streaming.MemoryStream
import io.delta.tables.DeltaTable
import scribe.*
import scribe.format.*

import scala.util.chaining.scalaUtilChainingOps






object DataTypes:

    type Location = String

    case class User(id: String, name: Option[String], location: Location)
    case class UserByLocationAggregate(location: Location, users: List[User])

    sealed abstract class Change[+T](val entity: T, val version: Long)
    case class Add[T](override val entity: T, override val version: Long)    extends Change[T](entity, version)
    case class Remove[T](override val entity: T, override val version: Long) extends Change[T](entity, version)

    case class RawChange[T](entity: T, changeType: String, commitVersion: Long, commitTimestamp: java.sql.Timestamp)


import DataTypes.*


class UserByLocationProcessor extends StatefulProcessor[Location, Change[User], UserByLocationAggregate] with Serializable:

    import io.github.pashashiz.spark_encoders.TypedEncoder.given

    @transient private var users: ValueState[List[User]] = uninitialized


    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit =
        users = getHandle.getValueState[List[User]]("users", TTLConfig.NONE)

    override def handleInputRows(
        key: Location,
        inputRows: Iterator[Change[User]],
        timerValues: TimerValues
    ): Iterator[UserByLocationAggregate] =


        val inputRowsList = inputRows.toList

        val current       = if users.exists() then users.get() else List.empty[User]

        // Order by commit version, and within a version apply Remove before Add
        val ordered       = inputRowsList.sortBy {
            case Remove(_, v) => (v, 0)
            case Add(_, v)    => (v, 1)
        }

        val next = ordered.foldLeft(current) {
            case (acc, Remove(u, _)) => acc.filterNot(_.id == u.id)
            case (acc, Add(u, _))    => acc.filterNot(_.id == u.id) :+ u
        }

        users.update(next)

        Iterator.single(UserByLocationAggregate(key, next))




object TWS2:

    Logger.root
          .clearHandlers()
          .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
          .replace()


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

    def createCdfDF(spark: SparkSession, tablePath: String, maxFilesPerTrigger: Int): DataFrame =
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

        val b0 = DeltaTable.create(spark).location(path
        ) // <-- must be an absolute path otherwise interpreted as a table name
        val b1 = encT.schema.fields.foldLeft(b0) { (b, f) =>
            b.addColumn(f.name, sqlType(f.dataType), f.nullable)
        }

        val b2 = if enableCDF then b1.property("delta.enableChangeDataFeed", "true") else b1
        b2.execute()
    }

    def entityStructFor[T](name: String = "entity")(using enc: Encoder[T]): Column =
        struct(enc.schema.fieldNames.map(col) *).as(name)

    def createUnsafeRawChangeDSFor[T](cdfDF: DataFrame)(using entityEnc: Encoder[T], rawChangeEnc: Encoder[RawChange[T]]): Dataset[RawChange[T]] =
        cdfDF.select(
          entityStructFor[T]("entity"),
          col("_change_type").as("changeType"),
          col("_commit_version").as("commitVersion"),
          col("_commit_timestamp").as("commitTimestamp")
        )
        .as[RawChange[T]]
        .tap { _.printSchema() }

    def computeSortedRawChangeDSFor[T](rawDS: Dataset[RawChange[T]])(using rawChangeEnc: Encoder[RawChange[T]]): Dataset[RawChange[T]] =
        rawDS
          .withColumn("orderHint", when(col("changeType").isin("delete", "update_preimage"), lit(0)).otherwise(lit(1)))
          .orderBy(col("commitVersion").asc, col("orderHint").asc)
          .drop("orderHint")
          .as[RawChange[T]]

    def computeChangeDSFor[T](raw: Dataset[RawChange[T]])(using Encoder[Change[T]]): Dataset[Change[T]] =
        raw.map { rc =>
            rc.changeType match
                case "insert" | "update_postimage" => Add(rc.entity, rc.commitVersion)
                case "delete" | "update_preimage" => Remove(rc.entity, rc.commitVersion)
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
          .appName("GroupBy Application")
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


        val tablePath = "data/delta/tws2"

        deleteTableIfExist(tablePath)

        //commit 0
        createDeltaTableFor[User](spark, Path(tablePath).absolute.toString, enableCDF = true)

        val deltaTable = DeltaTable.forPath(spark, tablePath)
        val memSrc     = MemoryStream[User]
        val userDS     = createDSFromMemSrcFor[User](memSrc, partition = 1)

        val userDSInToDeltaQuery = runUpsertUserDSIntoDelta(deltaTable, userDS)


//        val aggregationQuery = userChangeDS
//          .groupByKey(_.entity.location)
//          .transformWithState[UserByLocationAggregate](
//              new UserByLocationProcessor,
//              TimeMode.EventTime,
//              OutputMode.Update
//          )
//          .writeStream
//          .outputMode(OutputMode.Update)
//          .format("console")
//          .option("truncate", value = false)
//          .start()


//        "1st micro-batch" pipe println
//        memSrc.addData(
//            Add(User("1", "U1", "Paris"),    1L),
//            Add(User("2", "U2", "Paris"),    1L),
//            Add(User("3", "U3", "London"),   1L),
//            Add(User("4", "U4", "Brazzaville"), 1L),
//            Add(User("5", "U5", "New York"), 1L),
//            Add(User("7", "Maatari", "Madrid"), 1L)
//        )
//
//        aggregationQuery.processAllAvailable()
//
//        "2nd micro-batch" pipe println
//        memSrc.addData(
//            Add(User("6", "U6", "New York"), 2L),
//
//            // Move id=1: Paris -> London within commit version 2
//            Remove(User("1", "U1", "Paris"),  2L),
//            Add(User("1", "U1", "London"),     2L),
//
//            // Move id=5: New York -> Brazzaville within commit version 2
//            Remove(User("5", "U5", "New York"), 2L),
//            Add(User("5", "U5", "Brazzaville"), 2L)
//        )
//
//        aggregationQuery.processAllAvailable()
//
//        "3rd micro-batch: Madrid -> Paris -> London" pipe println
//        memSrc.addData(
//            // Move 7: Madrid -> Paris at version 3
//            Remove(User("7", "Maatari", "Madrid"), 3L),
//            Add(User("7", "Maatari", "Paris"),     3L),
//
//            // Move 7: Paris -> London at version 4
//            Remove(User("7", "Maatari", "Paris"),  4L),
//            Add(User("7", "Maatari", "London"),    4L)
//        )
//        aggregationQuery.processAllAvailable()




        // 6. Stop
        spark.stop()






















