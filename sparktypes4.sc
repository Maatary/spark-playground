import org.apache.spark.sql.types.*
import io.github.pashashiz.spark_encoders.TypedEncoder

import scala.util.chaining.scalaUtilChainingOps

sealed trait Person
case class Adult(name: String, age: Int, birthday: Option[Int]) extends Person
case class Child(name: String, age: Int, birthday: Option[Int], guardian: String) extends Person
case class Senior(name: String, age: Int, birthday: Option[Int], pensionId: String) extends Person


TypedEncoder[Person]
    .encoder
    .schema
    .printTreeString()



TypedEncoder[Adult]
    .encoder
    .schema
    .printTreeString()


case class Flight(
    DEST_COUNTRY_NAME  : String,
    ORIGIN_COUNTRY_NAME: String,
    count              : Long
)


TypedEncoder[Option[Flight]]
    .encoder
    .schema
    .tap {_.printTreeString()}
    .tap { _.sql pipe println}



case class value(flight: Flight)

TypedEncoder[value]
    .encoder
    .schema
    .printTreeString()

case class ID(id: Long)

TypedEncoder[ID]
    .encoder
    .schema
    .printTreeString()

"=========== User Change" pipe println

case class User(id: Long, name: String)
case class RawChange[T](
    entity: T,
    changeType: String,
    commitVersion: Long,
    commitTimestamp: java.sql.Timestamp
)

val enc = TypedEncoder[RawChange[User]]

val resolvedEnc = enc.encoder.resolveAndBind()

"====== Encoder & Schema" pipe println

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

"====== Deserializer" pipe println

resolvedEnc
    .deserializer
    .dataType

resolvedEnc
    .deserializer
    .numberedTreeString

resolvedEnc
    .deserializer
    .treeString

"====== Serializer" pipe println

resolvedEnc
    .serializer
    .foreach(e => println(e.dataType))


resolvedEnc
    .serializer
    .foreach(e => println(e.numberedTreeString))

resolvedEnc
    .serializer
    .foreach(e => println(e.treeString))




