//> using scala 3.7.0

//> using options -Xmax-inlines 128
//> using options -new-syntax
////> using options -explain


//> using javaOpt "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.nio=ALL-UNNAMED"

//> using repository m2Local


// //> using dep org.apache.spark:spark-sql_2.13:4.0.0
////> using dep org.apache.spark:spark-sql_2.13:4.0.1-SNAPSHOT

//> using dep "org.apache.spark:spark-sql_2.13:4.0.1-SNAPSHOT,exclude=org.apache.spark%spark-connect-shims_2.13,exclude=org.apache.logging.log4j%log4j-slf4j2-impl,exclude=org.slf4j%slf4j-reload4j,exclude=ch.qos.reload4j%reload4j"


////> using dep "io.github.vincenzobaz::spark-scala3-encoders:0.2.6,exclude=org.apache.spark%spark-connect-shims_2.13,exclude=org.apache.logging.log4j%log4j-slf4j2-impl,exclude=org.slf4j%slf4j-reload4j,exclude=ch.qos.reload4j%reload4j"
////> using dep "io.github.pashashiz::spark-encoders:0.1.0+4-186f80e9-SNAPSHOT,exclude=org.apache.spark%spark-connect-shims_2.13,exclude=org.apache.logging.log4j%log4j-slf4j2-impl,exclude=org.slf4j%slf4j-reload4j,exclude=ch.qos.reload4j%reload4j"
//> using dep "io.github.pashashiz::spark-encoders:0.1.0+6-16b57f93+20250903-1610-SNAPSHOT,exclude=org.apache.spark%spark-connect-shims_2.13,exclude=org.apache.logging.log4j%log4j-slf4j2-impl,exclude=org.slf4j%slf4j-reload4j,exclude=ch.qos.reload4j%reload4j"

////> using dep org.apache.hadoop:hadoop-client-api:3.3.6
//> using dep "org.apache.hadoop:hadoop-client:3.4.1,exclude=org.apache.logging.log4j%log4j-slf4j2-impl,exclude=org.slf4j%slf4j-reload4j,exclude=ch.qos.reload4j%reload4j"

////> using dep org.apache.hadoop:hadoop-client-runtime:3.3.6
//> using dep "org.apache.hadoop:hadoop-client-runtime:3.4.1,exclude=org.apache.logging.log4j%log4j-slf4j2-impl,exclude=org.slf4j%slf4j-reload4j,exclude=ch.qos.reload4j%reload4j"
////> using dep org.apache.hadoop:hadoop-aws:3.3.6

//> using dep "org.apache.hadoop:hadoop-aws:3.4.1,exclude=org.apache.logging.log4j%log4j-slf4j2-impl,exclude=org.slf4j%slf4j-reload4j,exclude=ch.qos.reload4j%reload4j"

//> using dep "org.typelevel::cats-core:2.13.0"
//> using dep "org.typelevel::cats-effect:3.6.3"
//> using dep "co.fs2::fs2-core:3.12.0"


//> using dep "com.outr::scribe:3.17.0" // Scribe core
//> using dep "com.outr::scribe-cats:3.17.0"
//> using dep "com.outr::scribe-slf4j2:3.17.0" // SLF4J API façade + Scribe binding


