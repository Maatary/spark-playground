

import io.github.pashashiz.spark_encoders.TypedEncoder
import io.github.pashashiz.spark_encoders.TypedEncoder.given
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.ArrayType

case class Attr(name: String, value: String)
case class Node(`_local_id`: String,`_urn`: String, attr: Option[List[Attr]] )
case class ResNet(nodes: List[Node])

//val nodeEnc = implicitly[ExpressionEncoder[Node]]
//
//
//nodeEnc.encoder.schema
//nodeEnc.encoder.schema.printTreeString()
//
//nodeEnc.schema
//nodeEnc.schema.printTreeString()
//
//val resolvedNodeEnc = nodeEnc.resolveAndBind()
//
//resolvedNodeEnc.schema
//resolvedNodeEnc.schema.printTreeString()
//
////StructType(StructField("_local_id",StringType,false),StructField("_urn",StringType,false),StructField("attr",ArrayType(StructType(StructField("name",StringType,false),StructField("value",StringType,false)),false),true))
//
//
val listNodeEnc = TypedEncoder[List[Node]].encoder

listNodeEnc.schema
listNodeEnc.schema.printTreeString()



val resnetEnc = TypedEncoder[ResNet].encoder

resnetEnc.schema
resnetEnc.schema.printTreeString()