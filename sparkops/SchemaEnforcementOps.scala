package sparkops

import io.github.pashashiz.spark_encoders.TypedEncoder
import io.github.pashashiz.spark_encoders.TypedEncoder.given
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*

object SchemaEnforcementOps:

    /** =Recover Schema Nullability after Loaders Weaken it.=
     *
     * Many DataFrame loaders (JSON, XML, etc.) materialize every field as nullable—even when the declared schema says otherwise. This method builds
     * a column expression which, when used in a select to project the original fields, yields a DataFrame whose inferred schema matches the original
     * schema, while the expression itself enforces that schema at runtime by failing fast on violations.
     *
     * More specifically, it uses the declared schema to build nullability-enforced column expressions. Because Catalyst (Spark’s analyzer) derives a
     * DataFrame’s schema from each column expression’s static type (`dataType`, `nullable`, and `containsNull`), the resulting schema realigns with
     * the declared schema.
     *
     * Mechanically, we encode nullability using the following coalesce expression pattern:
     *
     *   - {{{coalesce(FieldExpr, raise_error(...), NonNullLiteral)}}}
     *
     * The non-null literal exists solely to force static nullability (nullable=false / containsNull=false) during analysis; at runtime, valid inputs
     * pass unchanged and invalid inputs raise immediately.
     *
     * This pattern delivers two complementary effects:
     *
     *   - **Type Coercion (analysis-time)**: the injected non-null literal makes the expression statically non-nullable, allowing Catalyst to infer
     *     and assign the declared nullability (nullable = false / containsNull = false). The literal is a typing witness only and is never observed
     *     at runtime; valid inputs flow through unchanged.
     *   - **Type Enforcement (runtime)**: the `raise_error(...)` branch fails fast on null inputs, aborting with a clear message that includes the
     *     logical path and record-id.
     *
     * Note that datatypes remain unchanged because the weakened schema never alters them—it only relaxes nullability. The resulting DataFrame
     * therefore preserves the original field types exactly as declared.
     *
     * ==Special handling for array fields.==
     *
     * Arrays have **two nullability axes**: the *container* (`StructField.nullable`) and the *elements* (`ArrayType.containsNull`). Enforce them
     * **in this order**:
     *
     *   1. **Element rule — align element nullability first** (only when `containsNull = false`). We rewrite the array with a non-nullable
     *      `transform` lambda so Catalyst sees `containsNull = false`:
     *      {{{
     *     // buildNullabilityEnforcedArrayElementsExpr(arrayExpr, elemType, path, id)
     *     transform(arrayExpr,
     *       e => coalesce(e, raise_error(...), defaultLiteralFor(elemType)))
     *      }}}
     *      The non-null default makes the lambda non-nullable, so the transformed array becomes `ArrayType(elemType, containsNull = false)` at
     *      analysis time. (If `containsNull = true`, this step is a no-op and we keep the array as-is.)
     *
     * 2. **Container rule — enforce the array itself** (only when `field.nullable = false`). With element nullability already aligned, we make the
     * **container** non-nullable via the coalesce idiom and a typed empty array literal:
     * {{{
     *     // emptyArrayLiteral(elemType, containsNull) ⇒ array().cast(ArrayType(elemType, containsNull))
     *     coalesce(arrayWithEnforcedElements, raise_error(...), emptyArrayLiteral(elemType, containsNull))
     * }}}
     * The typed empty array literal ensures all branches advertise the **same `ArrayType`**; the expression is statically non-nullable, and any null
     * container fails via `raise_error(...)`.
     *
     * **Why the order matters.** Catalyst will not cast `ArrayType(t, true)` → `ArrayType(t, false)`. By proving elements non-null **first**, we
     * avoid any illegal down-nullability cast when we enforce the container. (If the container is nullable, we simply return the element-aligned
     * array without wrapping it in `coalesce`.)
     *
     * ==Struct containers (top-level struct fields).==
     *
     * Struct-typed fields also have a container nullability given by `StructField.nullable`. For
     * top-level struct fields (the `isRoot = true` case), we treat this as a container-level
     * contract:
     *
     *   - When `nullable = true`, a null struct is propagated as `null` and its children are not
     *     evaluated. This matches `Option[Struct]` semantics: either the container is absent, or
     *     present and we enforce nullability inside it.
     *
     *   - When `nullable = false`, the struct container itself must never be `null`. We build a
     *     projected struct expression from the recursively enforced child fields:
     *     {{{
     *       val structExpr = struct(childFieldExprs*)
     *     }}}
     *     and then enforce the container via a `when` + `coalesce` pattern:
     *     {{{
     *       coalesce(
     *         when(
     *           sourceStruct.isNull,
     *           raise_error(concat(lit(s"null value at column $path, id: "), idColumn))
     *         ).otherwise(structExpr),
     *         defaultStructLiteralFor(structType, path)
     *       )
     *     }}}
     *
     *     At analysis time the non-null default struct literal is used only as a typing witness to
     *     force `nullable = false` on the whole expression; it is never observed at runtime. At
     *     runtime either:
     *
     *       - the source struct is non-null, in which case `structExpr` is evaluated and the
     *         scalar/array rules above enforce the declared nullability of the child fields, or
     *       - the source struct is null, in which case `raise_error(...)` fails fast at the
     *         struct's logical path.
     *
     * Nested struct types (`StructType` fields inside non-root structs) are currently rejected
     * with an `IllegalArgumentException`, to keep the implementation focused on top-level
     * container recovery.
     *
     * As with scalars and arrays, these struct container expressions serve a dual purpose:
     * they realign Spark's inferred logical schema (including `nullable` flags) with the
     * declared contract, and they enforce that contract at runtime by failing fast on
     * violations.
     *
     * ==Usage Notes==
     *
     * Import `sparkutils.SchemaEnforcementOps.*` to use `df.enforceAs[T](idColumn)`.
     * The extension applies these rules using the encoder schema of `T`.
     *
     * @param sourceColumn the field expression from the loaded DataFrame
     * @param expectedType the field type declared by the encoder
     * @param logicalPath the field path included in validation errors
     * @param isRoot whether this is a top-level field
     * @param fieldNullable whether the declared field permits null
     * @param recordIdColumn the record identifier included in validation errors
     * @return the field expression enforcing the declared nullability
     */
    def buildSchemaEnforcedFieldExpr(
        sourceColumn: Column,
        expectedType: DataType,
        logicalPath: String,
        isRoot: Boolean,
        fieldNullable: Boolean,
        recordIdColumn: Column
    ): Column =
        (expectedType, isRoot) match
            case (structType: StructType, true) =>
                val fieldExpressions = structType.fields.map { field =>
                    val childPath   = s"$logicalPath.`${field.name}`"
                    val childColumn = sourceColumn.getField(field.name)

                    buildSchemaEnforcedFieldExpr(
                        sourceColumn = childColumn,
                        expectedType = field.dataType,
                        logicalPath = childPath,
                        isRoot = false,
                        fieldNullable = field.nullable,
                        recordIdColumn = recordIdColumn
                    ).as(field.name)
                }

                val structExpr = struct(fieldExpressions*)

                if fieldNullable then
                    when(sourceColumn.isNull, lit(null)).otherwise(structExpr)
                else
                    val defaultStructExpr = defaultStructLiteralFor(structType, logicalPath)
                    coalesce(
                        when(
                            sourceColumn.isNull,
                            raise_error(concat(lit(s"null value at column $logicalPath, id: "), recordIdColumn))
                        ).otherwise(structExpr),
                        defaultStructExpr
                    )

            case (structType: StructType, false) =>
                throw new IllegalArgumentException(
                    s"Nested struct handling not implemented for $logicalPath (type $structType)"
                )

            case (ArrayType(elementType, containsNull), _) =>
                val arrayWithEnforcedElements =
                    if containsNull then sourceColumn
                    else buildNullabilityEnforcedArrayElementsExpr(sourceColumn, elementType, s"$logicalPath[*]", recordIdColumn)

                if fieldNullable then arrayWithEnforcedElements
                else
                    coalesce(
                        arrayWithEnforcedElements,
                        raise_error(concat(lit(s"null value at column $logicalPath, id: "), recordIdColumn)),
                        emptyArrayLiteral(elementType, containsNull)
                    )

            case _ =>
                val field = StructField(name = logicalPath, dataType = expectedType, nullable = fieldNullable)
                buildNullabilityEnforcedScalarFieldExpr(sourceColumn, field, recordIdColumn)

    private def buildNullabilityEnforcedScalarFieldExpr(
        sourceColumn: Column,
        expectedField: StructField,
        recordIdColumn: Column
    ): Column =
        if expectedField.nullable then sourceColumn
        else
            coalesce(
                sourceColumn,
                raise_error(concat(lit(s"null value at column $sourceColumn, id: "), recordIdColumn)),
                defaultLiteralFor(expectedField.dataType)
            )

    private def buildNullabilityEnforcedArrayElementsExpr(
        arrayToTransform: Column,
        elementType: DataType,
        logicalPath: String,
        recordIdColumn: Column
    ): Column =
        transform(
            arrayToTransform,
            element =>
                coalesce(
                    element,
                    raise_error(concat(lit(s"null element at column $logicalPath, id: "), recordIdColumn)),
                    defaultLiteralFor(elementType)
                )
        )

    private def defaultLiteralFor(dt: DataType): Column = dt match
        case StringType    => lit("")
        case IntegerType   => lit(0)
        case LongType      => lit(0L)
        case DoubleType    => lit(0.0)
        case TimestampType => lit(new java.sql.Timestamp(0L))
        case BooleanType   => lit(false)
        case other         =>
            throw new IllegalArgumentException(s"Unsupported type in this helper: $other")

    private def emptyArrayLiteral(elementType: DataType, containsNull: Boolean): Column =
        array().cast(ArrayType(elementType, containsNull))

    private def defaultStructLiteralFor(structType: StructType, logicalPath: String): Column =
        struct(
            structType.fields.map {
                case StructField(name, arr: ArrayType, _, _) =>
                    emptyArrayLiteral(arr.elementType, arr.containsNull).as(name)

                case StructField(name, st: StructType, _, _) =>
                    throw new IllegalArgumentException(
                        s"default literal for nested struct not supported at $logicalPath.$name"
                    )

                case StructField(name, dt, _, _) =>
                    defaultLiteralFor(dt).as(name)
            }*
        )

    extension (df: DataFrame)

        /**
         * Restores and enforces the nullability contract declared by the encoder of T.
         *
         * Reprojects each top-level field so Spark's inferred logical schema matches
         * the encoder schema. The same expressions reject prohibited nulls during
         * evaluation, identifying the field and record. Valid values pass unchanged.
         *
         * {{{
         * import io.github.pashashiz.spark_encoders.TypedEncoder.given
         * import org.apache.spark.sql.functions.col
         * import sparkutils.SchemaEnforcementOps.*
         *
         * val enforcedDs: Dataset[T] = rawDf.enforceAs[T](idColumn = col("id"))
         * }}}
         *
         * @param idColumn the record identifier included in validation errors
         * @param enc the typed encoder whose schema defines the contract
         * @return the lazy Dataset with schema enforcement in its projection
         */
        def enforceAs[T](idColumn: Column)(using enc: TypedEncoder[T]): Dataset[T] =
            val fields = enc.encoder.schema.fields.toSeq.map { field =>
                buildSchemaEnforcedFieldExpr(
                    sourceColumn = col(field.name),
                    expectedType = field.dataType,
                    logicalPath = s"`${field.name}`",
                    isRoot = true,
                    fieldNullable = field.nullable,
                    recordIdColumn = idColumn
                ).as(field.name)
            }

            df.select(fields*).as[T]
