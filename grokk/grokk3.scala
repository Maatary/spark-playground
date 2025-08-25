// Compile / run with:  spark-submit --class Demo <jar>     (or run from IntelliJ / sbt)

package grokk.grokk3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import scribe.*
import scribe.format.*

import scala.util.chaining.*

object StreamingGroupByMemorySinkDemo {

    // ————————————————————————————————————————————————————————————————
    // Simple case class for our user records
    // ————————————————————————————————————————————————————————————————
    case class User(user_id: Long, location: String)

    def main(args: Array[String]): Unit = {

        Logger.root
            .clearHandlers()
            .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
            .replace()

        // 1. SparkSession in local mode
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

        // 2. MemoryStream source (requires an implicit SQLContext)
        implicit val sqlCtx = spark.sqlContext

        val memSrc = MemoryStream[User]

        // 3. Streaming DataFrame
        val users = memSrc.toDS() // Dataset[User] – isStreaming = true

        // 4. Aggregation in **Update** output mode
        val agg = users
            .groupBy($"location")
            .agg(collect_list($"user_id").as("users"))

        val query = agg.writeStream
            .outputMode(OutputMode.Update) // changelog-style: only rows that changed
            .format("memory")              // in-process sink we can query with SQL
            .queryName("agg_table")        // must give it a name for memory sink
            .start()

        // Helper to dump the current sink content
        def showSnapshot(batch: String): Unit = {
            println(s"\n== $batch ==")
            spark.sql("select * from agg_table order by location").show(false)
        }

        // 5-A. First micro-batch – initial 5 rows
        memSrc.addData(Seq(
          User(1, "Paris"),
          User(2, "Paris"),
          User(3, "London"),
          User(4, "Brazzaville"),
          User(5, "New York")
        ))
        query.processAllAvailable() // block until micro-batch finishes
        showSnapshot("after batch 1")

        // 5-B. Second micro-batch – *only* an “update” row for user 1
        memSrc.addData(Seq(
          User(1, "London") // no matching delete for Paris!
        ))
        query.processAllAvailable()
        showSnapshot("after batch 2")

        // 6. Clean up
        spark.stop()
    }
}

object StreamingGroupByConsoleDemo {

    case class User(user_id: Long, location: String)


    def main(args: Array[String]): Unit = {

        Logger.root
            .clearHandlers()
            .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
            .replace()

        // 1. SparkSession ----------------------------------------------------------
        val spark = SparkSession
            .builder()
            .appName("console-sink demo")
            .master("local[*]")
            .config("spark.sql.streaming.stateStore.providerClass",
                    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
            .config("spark.sql.shuffle.partitions", 12)
            .getOrCreate()

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        // 2. MemoryStream source ---------------------------------------------------
        implicit val sqlCtx = spark.sqlContext
        val memSrc          = MemoryStream[User]

        val users = memSrc.toDS() // Dataset[User] (isStreaming = true)

        // 3. Aggregation (changelog style) ----------------------------------------
        val agg = users
            .groupBy($"location")
            .agg(collect_list($"user_id").as("users"))

        val query = agg.writeStream
            .outputMode(OutputMode.Update) // emit rows that changed this batch
            .format("console")             // print to stdout
            .option("truncate", value = false) // show full arrays
            .start()

        // 4. First micro-batch -----------------------------------------------------
        memSrc.addData(
          User(1, "Paris"),
          User(2, "Paris"),
          User(3, "London"),
          User(4, "Brazzaville"),
          User(5, "New York")
        )
        query.processAllAvailable() // block until batch finished

        // 5. Second micro-batch (user 1 moves to London) --------------------------
        memSrc.addData(User(1, "London"))
        query.processAllAvailable()

        // 6. Stop ------------------------------------------------------------------
        spark.stop()
    }
}

object StreamingGroupByConsoleDemo2 {

    case class User(
        user_id: Long,
        location: String,
        timestamp: java.sql.Timestamp = new java.sql.Timestamp(System.currentTimeMillis())
    )

    def main(args: Array[String]): Unit = {

        Logger.root
            .clearHandlers()
            .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
            .replace()

        // 1. SparkSession ----------------------------------------------------------
        val spark = SparkSession
            .builder()
            .appName("console-sink demo")
            .master("local[*]")
            .getOrCreate()

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        // 2. MemoryStream source ---------------------------------------------------
        implicit val sqlCtx = spark.sqlContext
        val memSrc          = MemoryStream[User]

        val users = memSrc.toDS() // Dataset[User] (isStreaming = true)

        // 3. Aggregation (changelog style) ----------------------------------------
        val agg = users
            .withWatermark("timestamp", "100 seconds")
            .groupBy($"location")
            .agg(collect_list($"user_id").as("users"))
            .select($"location", $"users")

        val query = agg
            .writeStream
            .outputMode(OutputMode.Update) // emit rows that changed this batch
            .format("console")             // print to stdout
            .option("truncate", value = false) // show full arrays
            .start()

        // 4. First micro-batch -----------------------------------------------------
        memSrc.addData(
          User(1, "Paris"),
          User(2, "Paris"),
          User(3, "London"),
          User(4, "Brazzaville"),
          User(5, "New York")
        )
        query.processAllAvailable() // block until batch finished

        // 5. Second micro-batch (user 1 moves to London) --------------------------
        memSrc.addData(User(1, "London"))
        query.processAllAvailable()

        // 6. Stop ------------------------------------------------------------------
        spark.stop()
    }
}
