package grokk.grokk2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.streaming.{OutputMode, StatefulProcessor, TimeMode, TimerValues, ValueState}
import org.apache.spark.sql.streaming.*
import scribe.*
import scribe.format.*

object DataTypes {
    case class In(id: String, ts: java.sql.Timestamp, value: Long)
    case class Out(id: String, total: Long)
}


@main
def main(): Unit =

    val pretty: Formatter =
        formatter"""${cyan(bold(dateFull))}  ${italic(threadName)}  ${levelColored}  ${green(position)} ${messages}"""

//    Logger.root
//      .withHandler(formatter = pretty, minimumLevel = Some(Level.Error)) // no handler building needed
//      .replace()

    Logger.root
          .clearHandlers()
          .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
          .replace()

    scribe.info("Starting up")

    implicit val spark =
        SparkSession
          .builder
          .appName("stateful-demo")
          .master("local[*]")
          .config(
              "spark.sql.streaming.stateStore.providerClass",
              "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
          )
          .config("spark.sql.shuffle.partitions", 12)
          .getOrCreate()


    import spark.implicits._
    //import scala3encoders.given
    import io.github.pashashiz.spark_encoders.TypedEncoder.*
    import io.github.pashashiz.spark_encoders.TypedEncoder.given

    import DataTypes.*


    spark.streams.addListener(new org.apache.spark.sql.streaming.StreamingQueryListener {
        override def onQueryStarted(e: org.apache.spark.sql.streaming.StreamingQueryListener.QueryStartedEvent): Unit =  {
            println("=== DEBUG sessions seen in planner thread ===")
            println("Active  :" + SparkSession.getActiveSession)
            println("Default :" + SparkSession.getDefaultSession)
        }

        override def onQueryProgress(e: org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent): Unit = ()

        override def onQueryTerminated(e: org.apache.spark.sql.streaming.StreamingQueryListener.QueryTerminatedEvent): Unit = {
            println("=== DEBUG sessions seen in planner thread ===")
            println("Active  :" + SparkSession.getActiveSession)
            println("Default :" + SparkSession.getDefaultSession)
        }
    })

    /** Keeps a running sum per id */
    class SumProc extends StatefulProcessor[String, In, Out] with Serializable:
        private var sum: ValueState[Long] = _

        override def init(o: OutputMode, t: TimeMode): Unit =
            sum = getHandle.getValueState[Long]("sum", TTLConfig.NONE)

        override def handleInputRows(
                                      key: String,
                                      rows: Iterator[In],
                                      timers: TimerValues): Iterator[Out] =
            val current = if (sum.exists()) sum.get() else 0L
            val newTotal = current + rows.map(_.value).sum
            sum.update(newTotal)
            Iterator(Out(key, newTotal))

    import scala.util.chaining.scalaUtilChainingOps



    println(s"defaultParallelism = ${spark.sparkContext.defaultParallelism}")
    println(s"spark.sql.shuffle.partitions = ${spark.conf.get("spark.sql.shuffle.partitions")}")


    val stream = spark
      .readStream
      .format("rate-micro-batch")
      .option("rowsPerBatch", 100)
      .load()
      .select(
          (col("value") % 10).cast("string").as("id"),
          col("timestamp").as("ts"),
          lit(1L).as("value")
      )
      //.selectExpr("CAST(value AS STRING) id", "timestamp ts", "1L value")
      .as[In]
      .tap{_.explain(true)}

    stream
      .groupByKey(_.id)
      .transformWithState[Out](
          new SumProc,
          TimeMode.EventTime,
          OutputMode.Update)
      .tap {_.explain(true)}
      .writeStream
      .format("console")
      .start()
      .awaitTermination()


