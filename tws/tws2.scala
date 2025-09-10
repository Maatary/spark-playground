package tws2

import scala.compiletime.uninitialized
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StatefulProcessor, TimeMode, TimerValues, ValueState}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.streaming.*
import org.apache.spark.sql.execution.streaming.MemoryStream
import scribe.*
import scribe.format.*

import scala.util.chaining.scalaUtilChainingOps






object DataTypes:

    type Location = String

    case class User(id: String, name: String, location: Location)
    case class UserByLocationAggregate(location: Location, users: List[User])

    // CDC aggregator phases with commit ordering
    sealed abstract class Change[+T](val entity: T, val version: Long)
    case class Add[T](override val entity: T, override val version: Long)    extends Change[T](entity, version)
    case class Remove[T](override val entity: T, override val version: Long) extends Change[T](entity, version)




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

    def main(args: Array[String]): Unit =

        Logger.root
              .clearHandlers()
              .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
              .replace()

        val spark = SparkSession
            .builder()
            .appName("Memory-source groupBy demo")
            .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
            .config("spark.sql.shuffle.partitions", 12)
            .master("local[*]")
            .getOrCreate()

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        implicit val sqlCtx = spark.sqlContext

        val memSrc = MemoryStream[Change[User]]
        val userChangeDS = memSrc.toDS()


        val aggregationQuery = userChangeDS
            .groupByKey(_.entity.location)
            .transformWithState[UserByLocationAggregate](
                new UserByLocationProcessor,
                TimeMode.EventTime,
                OutputMode.Update
            )
            //.tap { _.explain(true) }
            .writeStream
            .outputMode(OutputMode.Update)
            .format("console")
            .option("truncate", value = false)
            .start()


        "1st micro-batch" pipe println
        memSrc.addData(
            Add(User("1", "U1", "Paris"),    1L),
            Add(User("2", "U2", "Paris"),    1L),
            Add(User("3", "U3", "London"),   1L),
            Add(User("4", "U4", "Brazzaville"), 1L),
            Add(User("5", "U5", "New York"), 1L),
            Add(User("7", "Maatari", "Madrid"), 1L)
        )

        aggregationQuery.processAllAvailable()

        "2nd micro-batch" pipe println
        memSrc.addData(
            Add(User("6", "U6", "New York"), 2L),

            // Move id=1: Paris -> London within commit version 2
            Remove(User("1", "U1", "Paris"),  2L),
            Add(User("1", "U1", "London"),     2L),

            // Move id=5: New York -> Brazzaville within commit version 2
            Remove(User("5", "U5", "New York"), 2L),
            Add(User("5", "U5", "Brazzaville"), 2L)
        )

        aggregationQuery.processAllAvailable()

        "3rd micro-batch: Madrid -> Paris -> London" pipe println
        memSrc.addData(
            // Move 7: Madrid -> Paris at version 3
            Remove(User("7", "Maatari", "Madrid"), 3L),
            Add(User("7", "Maatari", "Paris"),     3L),

            // Move 7: Paris -> London at version 4
            Remove(User("7", "Maatari", "Paris"),  4L),
            Add(User("7", "Maatari", "London"),    4L)
        )
        aggregationQuery.processAllAvailable()

        // 6. Stop
        spark.stop()






















