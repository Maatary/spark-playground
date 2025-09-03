
import io.github.pashashiz.spark_encoders.TypedEncoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.*

import scala.util.chaining.*


TypedEncoder[Int].encoder

TypedEncoder[Int]
    .encoder
    .schema
    .printTreeString()

TypedEncoder[Int]
    .encoder
    .encoder

TypedEncoder[Int]
    .encoder
    .encoder
    .schema
    .printTreeString()


TypedEncoder[Option[Int]].encoder

TypedEncoder[Option[Int]]
    .encoder
    .schema
    .printTreeString()

TypedEncoder[Option[Int]]
    .encoder

TypedEncoder[Option[Int]]
    .encoder
    .schema.
    printTreeString()


sealed trait Person
case class Adult(name: String, age: Int, birthday: Option[Int]) extends Person
case class Child(name: String, age: Int, birthday: Option[Int], guardian: String) extends Person
case class Senior(name: String, age: Int, birthday: Option[Int], pensionId: String) extends Person


/**
 * == Agnostic Encoder Schema Vs Expression Encoder Schema ==
 */

val aEnc = TypedEncoder[Adult].encoder

val resolvedEnc = aEnc.resolveAndBind()

resolvedEnc
    .schema //serializer schema
    .printTreeString()

resolvedEnc
    .encoder
    .schema //Agnostic encoder schema
    .printTreeString()

resolvedEnc
  .encoder
  .isStruct

resolvedEnc
  .encoder
  .lenientSerialization
"======" pipe println

resolvedEnc
    .deserializer
    .dataType

resolvedEnc
    .deserializer
    .numberedTreeString

resolvedEnc
    .deserializer
    .treeString

"======" pipe println

resolvedEnc
    .serializer
    .foreach(e => println(e.dataType))


resolvedEnc
    .serializer
    .foreach(e => println(e.numberedTreeString))

resolvedEnc
    .serializer
    .foreach(e => println(e.treeString))


"======" pipe println

val adult1 = Adult("John", 30, None)
val adult2 = Adult("Smith", 40, Some(2))


val toRow  = aEnc.createSerializer()
val fromRow = aEnc.createDeserializer()

toRow(adult1)
toRow(adult2)