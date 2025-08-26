import org.apache.spark.sql.types.*
import io.github.pashashiz.spark_encoders.TypedEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder

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
    .tap {_.schema.printTreeString()}


TypedEncoder[Adult]
    .encoder
    .tap {_.schema.printTreeString()}


val personChangeEnc = TypedEncoder[Change[Person]]
    .encoder
    .resolveAndBind()
    .tap {_.schema.printTreeString()}


val toInternalRow           = personChangeEnc.createSerializer()
val fromInternalRow         = personChangeEnc.createDeserializer()
val insertAdult             = Insert(Adult("John", 30, None))
val personChangeInternalRow = toInternalRow(insertAdult)
val deserialized            = fromInternalRow(personChangeInternalRow)

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.Row

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder



/**
 * == InternalRow -> GenericRowWithSchema with Internal Catalyst utilities ==
 *
 * GenericRowWithSchema is a concrete implementation of '''sql.Row'''
 * as found in Dataset[Row] = DataFrame
 *
 * ''Note: No need to resolve and bind here''
 *
 * This is just for exploration purposes, not for production. It is for rapid testing,
 * for a situation where we want to test encoding and decoding without the full spark machinery
 *
 */
val catalystToExternalRow = CatalystTypeConverters.createToScalaConverter(personChangeEnc.schema)
val dfRow1                = catalystToExternalRow(personChangeInternalRow).asInstanceOf[Row]

dfRow1 pipe println


/**
 * == InternalRow -> GenericRowWithSchema with User-facing API ==
 *
 * GenericRowWithSchema is a concrete implementation of '''sql.Row'''
 * as found in Dataset[Row] = DataFrame
 *
 * This is just for exploration purposes, not for production. It is for rapid testing,
 * for a situation where we want to test encoding and decoding without the full spark machinery
 *
 *
 * ``RowEncoder.encoderFor(personChangeEnc.schema)`` can always be useful tho
 *
 */

val rowEncoderToExternalRow = ExpressionEncoder(RowEncoder.encoderFor(personChangeEnc.schema))
    .resolveAndBind()
    .createDeserializer()

val dfRow2 = rowEncoderToExternalRow(personChangeInternalRow)

println(personChangeEnc.schema.treeString)
println(dfRow2)

