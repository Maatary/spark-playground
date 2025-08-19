import org.apache.spark.sql.types.*
import io.github.pashashiz.spark_encoders.TypedEncoder

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
    count              : Int
)


TypedEncoder[Option[Flight]]
    .encoder
    .schema
    .printTreeString()


case class value(flight: Flight)

TypedEncoder[value]
    .encoder
    .schema
    .printTreeString()