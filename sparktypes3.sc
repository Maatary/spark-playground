import io.github.pashashiz.spark_encoders.TypedEncoder
import org.apache.spark.sql.types.*
import io.github.pashashiz.spark_encoders.TypedEncoder.given

/**
 * == Using Manual Schema: StructType ==
 *
 * Terrible thing to do, use encode schema instead, however good to know
 * how it works.
 *
 */

StructType(
    Seq(StructField("a", IntegerType),
        StructField("b", IntegerType)
    )
).printTreeString()

case class A(a: Int, b: Int)
TypedEncoder[A]
    .encoder
    .schema
    .printTreeString()




StructType(
    List(
        StructField("a", IntegerType, false),
        StructField("b", ArrayType(IntegerType, false), false)
    )
).printTreeString()

case class B(a: Int, b: List[Int])
TypedEncoder[B]
    .encoder
    .schema
    .printTreeString()



