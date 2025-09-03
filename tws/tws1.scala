import scala.compiletime.uninitialized
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StatefulProcessor, TimeMode, TimerValues, ValueState}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.streaming.*
import org.apache.spark.sql.execution.streaming.MemoryStream

import scribe.*
import scribe.format.*






object DataTypes:

    type Location = String

    case class User(id: String, name: String, location: Location)
    case class UserByLocationAggregate(location: Location, users: List[User])

    sealed abstract class Change[+T](val entity: T)
    case class Insert[T](override val entity: T) extends Change[T](entity)
    case class Update[T](override val entity: T) extends Change[T](entity)
    case class Delete[T](override val entity: T) extends Change[T](entity)




import DataTypes.*


class UserByLocationProcessor extends StatefulProcessor[Location, Change[User], UserByLocationAggregate] with Serializable:

    import io.github.pashashiz.spark_encoders.TypedEncoder.given

    private var users: ValueState[List[User]] = uninitialized


    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit =
        users = getHandle.getValueState[List[User]]("users", TTLConfig.NONE)

    override def handleInputRows(
        key: Location,
        inputRows: Iterator[Change[User]],
        timerValues: TimerValues
    ): Iterator[UserByLocationAggregate] =

       val current  = if users.exists() then users.get() else List.empty[User]
       val newUsers = inputRows map { case Insert(user) => user}
       val next     = current.appendedAll(newUsers)

       users.update(next)

       Iterator(UserByLocationAggregate(key, next))




object TWS1:

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

        val userChangeDS = MemoryStream[Change[User]].toDS()


        userChangeDS
          .explain(true)




























