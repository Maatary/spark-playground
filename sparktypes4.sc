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