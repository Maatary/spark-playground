
import io.github.pashashiz.spark_encoders.TypedEncoder
import io.github.pashashiz.spark_encoders.TypedEncoder.given
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*

import scala.util.chaining.*

/**
 * = Unifying principle: everything is an expression.=
 *
 * The Structured API is a Column Expression Language.
 * A Column Expression is a description of how to compute a column value (an expression).
 * Column expressions are trees (literal, column reference, function/operator, alias, field/element access).
 * They mirror the engine’s tree, but you can reason entirely at the API level.
 *
 * == What the API builds ==
 *
 * --------------------------
 *
 * - Column is a builder/wrapper for an expression in the Structured API's expression language
 *   (the DataFrame expression AST, a.k.a. ColumnNode). Evaluation is deferred.
 * - Spark Classic translates this AST to Catalyst; you don’t need Catalyst to reason correctly.
 * Example:
 *
 * {{{
 * (((col("someCol") + 5) * 200) - 6) < col("otherCol")
 *
 * Logical tree:
 *
 *         <
 *       /   \
 *      -   OtherCol
 *    /   \
 *   *     6
 *  / \
 * +  200
 * / \
 *SomeCol 5
 * }}}
 *
 * == Column reference expression ==
 *
 * --------------------------------------
 *
 * - Purpose:  build a *lookup-by-name* expression.
 *   Plain English:
 *    - Copy the input column named "x"
 *    - Compute a column by copying the input column named “x” — the identity recipe.
 *    - Produce a column named x by copying a column of the same name in the row passed to the expression as input.
 *    - It says build a column* by looking a column of a specific name and copying it
 *
 * - Why names? `col(name: String)` is a *column reference expression builder*; as such it expects
 *   a reference (a column name), not an expression.
 * - It says build a column* by looking a column of a specific name and copying it
 *
 * - Forms (equivalent for simple names):
 *    - col("x")            // unqualified column reference
 *    - expr("x")           // SQL fragment parsed into the same reference
 *    - df.col("x")         // qualified to df (helps disambiguate after joins)
 *
 * - Note: these references are *unresolved when built*; analysis later binds them to concrete inputs.
 *
 * ===Field/element access expression===
 *
 * -------------------------------
 *
 * - Extension of a column reference: first refer to a column, then access inside it.
 * - Structs:
 *   - col("s.a.b")            // dotted path parsed into parts
 *   - col("s")("a")("b")      // explicit field extraction
 * - Arrays / maps:
 *   - col("arr")(0)           // array element
 *   - col("m")("k")           // map value by key
 * - Quoting (for dots/spaces/keywords in names):
 *   - expr("`a.b`") or col("`a.b`")   // treat "a.b" as a single literal column name
 *
 * ===Selectors (using composite columns)===
 *
 * -----------------------------------
 *
 * - These build selector expressions; they do not evaluate anything at build time.
 * - Struct:
 *     col("s")("a")    // select field "a" from struct column "s"
 *     col("s.a")       // same via dotted path
 * - Array:
 *     col("arr")(i)    // select element at index i
 * - Map:
 *     col("m")("k")    // select value at key "k"
 * - Plain English: each is just another column expression; at runtime, given a row r,
 *   the selector reads inside the composite value from r and returns the selected part.
 *
 * ===Names vs expressions (don’t mix them)===
 *
 * -------------------------------------
 *
 * - col(name: String): Column          // expects a *name* (identifier) only
 *   - col("x") ✅ ; col("x + 1") ❌ (not a name)
 * - expr(sql: String): Column          // parses an *expression* (any term)
 *   Examples:
 *     expr("x")       == col("x")
 *     expr("x + 1")   == col("x") + lit(1)
 *     expr("x as y")  == col("x").as("y")
 *
 * ===Literals===
 *
 * --------
 *
 * - lit(v: Any): Column                // builds a literal (closed) expression; not an “attribute”
 *
 *
 * ===API expectations (typed)===
 *
 * ------------------------
 *
 *
 * - select(colNames: String*): DataFrame          // names only
 * - select(cols: Column*): DataFrame              // expressions
 * - selectExpr(sqlExprs: String*): DataFrame      // SQL fragments parsed into Column trees
 * - withColumn(colName: String, colExpr: Column): DataFrame   // name + expression
 *
 * ===Short laws to remember===
 *
 * ----------------------
 *
 *
 * - expr("x")            == col("x")
 * - expr("x + 1")        == col("x") + lit(1)
 * - expr("x = y")        == (col("x") === col("y"))
 * - expr("sum(x)")       == sum(col("x"))        // in agg context
 *
 * ===Notes on $"x"===
 *
 * -------------
 *
 *
 * - spark.implicits._  => $"x" is a Column (high-level API).
 * - catalyst DSL       => $"x" is a Catalyst UnresolvedAttribute (low-level). Don’t mix unless intentional.
 *
 * ===Binding (plain English)===
 *
 * -----------------------
 *
 *
 * - Column references and field/element accesses are *unresolved* when built.
 * - During analysis Spark binds them to concrete input attributes (or errors if missing/ambiguous).
 * - After binding they’re typed and later become position-based lookups at execution time.
 *
 */


val col1 = col("col1")
col1.explain(true)
col1.node
col1.node

//can't do that without spark session implicit
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
$"col2"
import org.apache.spark.sql.catalyst.expressions.Literal
Literal(1).eval()

lit(1).node
lit(3).as("col3").node

//Expression
expr("2 + 2").explain(true)
expr("2 + 2").node
expr("2 + 2")

expr("col1")

//Column Expression
col("col1") + 1
(col("col1") + col("col2")).node

(col("col1") + col("col2")).node.sql