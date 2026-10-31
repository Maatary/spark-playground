package biomed3471.claude

import biomed3471.claude.DataTypes.ForwardFlowSource
import io.delta.implicits.*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import scribe.{Level, Logger}
import sparkops.SchemaEnforcementOps.*

import scala.util.chaining.scalaUtilChainingOps

object ExploreApp:

    Logger.root
        .clearHandlers()
        .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
        .replace()

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("ExploreApp")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .config("spark.sql.shuffle.partitions", 12)
            .master("local[*]")
            .getOrCreate()

    def main(args: Array[String]): Unit =
        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import io.github.pashashiz.spark_encoders.TypedEncoder.*

        import DataTypes.*

//        val relationOccurrencesPath = "s3://cmdp-biomed-lakehouse-bucket-dev/m_okouya/BIOMED-3471/wytze-relation-occurrences/"
//
//        val relationOccurrences = spark
//            .read
//            .delta(relationOccurrencesPath)
//            .enforceAs[RelationOccurrence](lit(null))
//            .tap { _.toDF.printSchema() }
//
//        relationOccurrences
//            //.filter($"node1_name" === "parathyroid hormone" || $"node2_name" === "parathyroid hormone")
//            .filter($"direction" === "undirected")
//            .show(10, truncate = false)

//        val canonicalsPath = "s3://cmdp-biomed-lakehouse-bucket-dev/d_okouya/BIOMED-3471/2026-10-30/canonicalRelations"
//
//        val canonicals = spark
//            .read
//            .delta(canonicalsPath)
//            .enforceAs[CanonicalRelation]($"canonicalId")
//            .tap { _.printSchema() }
//
//        canonicals
////            .filter($"node1_name" === "parathyroid hormone" || $"node2_name" === "parathyroid hormone")
////            .withColumn("node1_name", $"node1_name")
//            .filter(col("sources") === array(lit(ForwardFlowSource)))
//            .tap { _.show(10, truncate = false) }
//            .tap { df => println(s"Canonical relations ${df.count()}") }

//        val nonIdenticalTwinPairs = spark
//            .read
//            .format("delta")
//            .load("s3://cmdp-biomed-lakehouse-bucket-dev/d_okouya/BIOMED-3471/2026-10-30/nonIdenticalTwinPairs")
//            .tap {_.printSchema()}
//            .tap { _.show(10, truncate = false) }
//
//
//        nonIdenticalTwinPairs
//            .count()
//            .tap { count => println(s"Non-identical twin pairs count: $count") }
//
//
//
//        val forwardRelationsWithTwins = nonIdenticalTwinPairs
//                .select("forwardTwin.*")
//                .distinct()
//
//        forwardRelationsWithTwins
//            .count()
//            .tap { count => println(s"Forward relations with twins count: $count") }
//
//
//        val backfillRelationsWithTwins = nonIdenticalTwinPairs
//                .select("backfillTwin.*")
//                .distinct()
//
//        backfillRelationsWithTwins
//            .count()
//            .tap { count => println(s"Backfill relations with twins count: $count") }



        val twinPairsWithNameMatchedNodePairTuples = spark
            .read
            .format("delta")
            .load("s3a://cmdp-biomed-lakehouse-bucket-dev/d_okouya/BIOMED-3471/2026-10-30/twinPairsWithNameMatchedNodePairTuples")
            .tap { _.printSchema() }
            .tap { _.show(10, truncate = false) }
            .tap { df => println(s"Twin pairs with name-matched node pair tuples count: ${df.count()}") }


        val homonymousNodePairs = spark
            .read
            .format("delta")
            .load("s3a://cmdp-biomed-lakehouse-bucket-dev/d_okouya/BIOMED-3471/2026-10-30/homonymousNodePairs")
            .tap { _.printSchema() }
            .tap { _.show(10, truncate = false) }
            .tap { df => println(s"Homonymous node pairs count: ${df.count()}") }
            .tap { df =>
                df.select("forwardNodeUri", "backfillNodeUri", "matchedNodeName")
                    .distinct()
                    .pipe { df => println(s"Distinct homonymous node pairs count: ${df.count()}") }
            }

//        val twinPairCountPerHomonymousNodePair = spark
//            .read
//            .format("delta")
//            .load("s3a://cmdp-biomed-lakehouse-bucket-dev/d_okouya/BIOMED-3471/2026-10-30/twinPairCountPerHomonymousNodePair")
//            .tap {_.printSchema()}
//            .tap { _.show(10, truncate = false) }

        spark.stop()
