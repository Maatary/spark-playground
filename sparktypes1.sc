
import io.github.pashashiz.spark_encoders.TypedEncoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.*

import scala.util.chaining.*


case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: Int)

TypedEncoder[Flight].encoder.schema pipe println


sealed trait Person

case class Adult(name: String, age: Int, birthday: Option[Int]) extends Person
case class Child(name: String, age: Int, birthday: Option[Int], guardian: String) extends Person
case class Senior(name: String, age: Int, birthday: Option[Int], pensionId: String) extends Person

TypedEncoder[Int].encoder.schema

TypedEncoder[Int].encoder.encoder.schema

val optEnc = TypedEncoder[Option[Int]].encoder

optEnc.encoder.schema

optEnc.schema

optEnc.schema.sql

optEnc.schema.printTreeString()

optEnc.schema.prettyJson


val aEnc = TypedEncoder[Adult].encoder

val resolvedEnc = aEnc.resolveAndBind()

resolvedEnc
    .schema //serializer schema
    .printTreeString()

resolvedEnc
    .encoder
    .schema //Agnostic encoder schema
    .printTreeString()

"======"

resolvedEnc
    .deserializer
    .dataType

resolvedEnc
    .deserializer
    .numberedTreeString

resolvedEnc
    .deserializer
    .treeString

"======="

resolvedEnc
    .serializer
    .foreach(e => println(e.dataType))


resolvedEnc
    .serializer
    .foreach(e => println(e.numberedTreeString))

resolvedEnc
    .serializer
    .foreach(e => println(e.treeString))


"======"

val adult1 = Adult("John", 30, None)
val adult2 = Adult("Smith", 40, Some(2))




val toRow  = aEnc.createSerializer()
val fromRow = aEnc.createDeserializer()

toRow(adult1)
toRow(adult2)

import io.github.pashashiz.spark_encoders.TypedEncoder.given

case class Attr(name: String, value: String)

case class Node(`_local_id`: String,`_urn`: String, attr: Option[List[Attr]] )

val nodeEnc = implicitly[ExpressionEncoder[Node]]


nodeEnc.schema


nodeEnc.schema.printTreeString()

val resolvedNodeEnc = nodeEnc.resolveAndBind()

resolvedNodeEnc.schema.toDDL

resolvedNodeEnc.schema.printTreeString()

//StructType(StructField("_local_id",StringType,false),StructField("_urn",StringType,false),StructField("attr",ArrayType(StructType(StructField("name",StringType,false),StructField("value",StringType,false)),false),true))