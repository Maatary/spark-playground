package dfp.dfp1
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





object DFP1:

    Logger.root
          .clearHandlers()
          .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
          .replace()

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("delta Application")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
            .config("spark.sql.shuffle.partitions", 12)
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .master("local[*]")
            .getOrCreate()


    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import _root_.io.github.pashashiz.spark_encoders.TypedEncoder.given

        val rsIdDF = Seq(
            "rs1026389333",
            "rs951523830",
            "rs2521707112",
            "rs1640862112",
            "rs1369767662",
            "rs1419901784",
            "rs1644023230",
            "rs1638376373",
            "rs1230880466",
            "rs1025529886"
        ).toDF("rs_id")


        val genVarDf = spark
            .read
            .format("delta")
            .load("s3a://cmdp-biomed-lakehouse-bucket-dev/e_ganeva/dataset-importer/geneticvariant/node-attribute-datasets/data/snp_vcf/")

//        genVarDf
//          .select($"_c2")
//          .tap(_.explain(true))
//          .show(false)

        val genVarSubset =
          genVarDf
            .join(broadcast(rsIdDF), genVarDf("_c2") === rsIdDF("rs_id"), "left_semi")
            .filter($"_c2".isin(rsIdDF.select("rs_id").as[String].collect(): _*))

        genVarSubset
          .tap(_.explain(true))
//          .show(false)




        Thread.sleep(Int.MaxValue)


        spark.stop()



object DFP2:

    Logger.root
          .clearHandlers()
          .withHandler(minimumLevel = Some(Level.Error)) // no handler building needed
          .replace()

    def makeSparkSession: SparkSession =
        SparkSession
            .builder()
            .appName("delta Application")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
            .config("spark.sql.shuffle.partitions", 12)
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
            .master("local[*]")
            .getOrCreate()


    def main(args: Array[String]): Unit =

        val spark = makeSparkSession

        import spark.implicits.{localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn, symbolToColumn}
        import _root_.io.github.pashashiz.spark_encoders.TypedEncoder.given

        val rsIdDF = Seq(
            "rs1026389333",
            "rs951523830",
            "rs2521707112",
            "rs1640862112",
            "rs1369767662",
            "rs1419901784",
            "rs1644023230",
            "rs1638376373",
            "rs1230880466",
            "rs1025529886",
            "rs226",
            "rs228",
            "rs318426",
            "rs385955",
            "rs2295766",
            "rs2295768 "
        ).toDF("rs_id")

        rsIdDF
         .write
         .mode("overwrite")
         .format("delta")
         .save("s3a://cmdp-biomed-lakehouse-bucket-dev/m_okouya/genetic-variant/rsIds")


//        val genVarDf = spark
//            .read
//            .format("delta")
//            .load("s3a://cmdp-biomed-lakehouse-bucket-dev/m_okouya/dataset-importer/geneticvariant/node-attribute-datasets/data/snp_vcf/")
//

//        val genVarSubset =
//            genVarDf
//                .join(broadcast(rsIdDF), genVarDf("_c2") === rsIdDF("rs_id"), "left_semi")
//                .filter($"_c2".isin(rsIdDF.select("rs_id").as[String].collect(): _*))

//        genVarSubset
//            .tap(_.explain(true))
//        //          .show(false)




        //Thread.sleep(Int.MaxValue)


        spark.stop()

