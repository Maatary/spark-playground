import org.apache.spark.sql.types.*
import io.github.pashashiz.spark_encoders.TypedEncoder

import scala.util.chaining.scalaUtilChainingOps

sealed trait Person
case class Adult(name: String, age: Int, birthday: Option[Int]) extends Person
case class Child(name: String, age: Int, birthday: Option[Int], guardian: String) extends Person
case class Senior(name: String, age: Int, birthday: Option[Int], pensionId: String) extends Person

sealed trait Change[+T](value: T)
case class Insert[T](value: T) extends Change[T](value)
case class Update[T](value: T) extends Change[T](value)
case class Delete[T](value: T) extends Change[T](value)

TypedEncoder[Person]
    .encoder
    .schema
    .printTreeString()


TypedEncoder[Adult]
    .encoder
    .schema
    .printTreeString()


TypedEncoder[Change[Person]]
    .encoder
    .schema
    .printTreeString()


val personChangerSer = TypedEncoder[Change[Person]]
    .encoder
    .createSerializer()

val insertAdult = Insert(Adult("John", 30, None))

val personInternalRow = personChangerSer(insertAdult)