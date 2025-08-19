import org.apache.spark.sql.types.*


StructType(
    Seq(
        StructField("a", IntegerType),
        StructField("b", IntegerType)
    )
).printTreeString()


