package catalyst.catalyst1


import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, SparkSession}

//if you need it
//import scribe.*
//import scribe.format.*

import scribe.*
import scribe.format.*


/**
 * == Constant Folding (Catalyst) — quick cheat‑sheet ==
 *
 * **What**
 * Catalyst precomputes deterministic, literal‑only sub‑expressions during optimization
 * and replaces them with a single `Literal`. This removes per‑row work.
 *
 * **Where**
 * Rule: `org.apache.spark.sql.catalyst.optimizer.ConstantFolding`
 * Phase: **after** Analysis (types/casts resolved), **before** Physical planning
 *
 * ---
 * ## Spot it in plans
 * ```text
 * -- SQL / Column API ------------------------------------------
 * == Optimized Logical Plan ==
 * Project [(id#0L + 7) AS folded#1L]      // 1 + 2*3  →  7
 *
 * -- Dataset.map (opaque lambda) -------------------------------
 * == Optimized Logical Plan ==
 * SerializeFromObject [input[0, bigint, false] AS value#…]
 * +- MapElements <lambda>, class java.lang.Long, [StructField(value,LongType,true)], obj#…: bigint
 * +- DeserializeToObject static_invoke(java.lang.Long.valueOf(id#…)), obj#…: java.lang.Long
 * // No (id + 7) here — lambda body isn't a Catalyst expression.
 * ```
 *
 * ---
 * ## Rules of thumb
 * - All children are **literals** and the expression is **deterministic** ⇒ foldable.
 * - Analyzer may insert **casts** first; folding happens after types line up.
 * - Inside `Dataset.map`/UDFs the math is JVM code ⇒ **not** foldable.
 *
 * ---
 * ## Keep folding while staying typed
 * ```scala
 * val ds: Dataset[Long] =
 * spark.range(10)
 * .select( ($"id" + (lit(1) + lit(2) * lit(3))).as("folded") )
 * .as[Long]
 * ```
 *
 * ---
 * ## Tiny checklist
 * - ❓ Only literals?  → Yes → fold.
 * - ❓ Deterministic?  → Yes → fold.
 * - ❓ Types aligned?  → Analyzer adds casts → then fold.
 *
 * @see org.apache.spark.sql.catalyst.optimizer.ConstantFolding
 */
object catalyst11:

    Logger.root
          .clearHandlers()
          .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
          .replace()

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("Catalyst Application")
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit = {

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.given

        spark
            .range(10)
            .selectExpr("1 + 2 * 3 + id as folded")
            .explain(true)

//        spark
//            .range(10)
//            .select(lit(1) + lit(2) + lit(3) + $"id" as "folded")
//            .explain(true)

        spark
            .range(10)
            .map(x => 1 + 2 * 3 + x)
            .explain(true)
    }

