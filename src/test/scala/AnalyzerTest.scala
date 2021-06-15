import org.scalatest.matchers.must.Matchers
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
case class OrderItem(
    productId: String,
    storeId: String,
    customerId: String,
    itemSku: String,
    quantity: Int,
    price: Double
);

class AnalyzerTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  val spark =
    SparkSession.builder
      .appName("Clark")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

  var schema: StructType =
    ScalaReflection.schemaFor[OrderItem].dataType.asInstanceOf[StructType]

  val tableDefs = List(new TableDef(schema, "test"))

  override protected def afterAll(): Unit = spark.stop()

  def runAnalyzer(schema: StructType, sql: String) = {
    val analyzer =
      new ColumnLineageAnalyzer(tableDefs)
    analyzer.analyze(sql);
    println("Lineage:\n" + analyzer.getOutputColumnLineage().mkString("\n"))

  }

  test("select * from test") {
    runAnalyzer(schema, "select * from test")
  }

  test("select sum(quantity) from test") {
    runAnalyzer(schema, "select sum(quantity) from test");
  }

  test("select sum(quantity) from test group by customerId") {
    runAnalyzer(schema, "select sum(quantity) from test group by customerId");
  }

  test(
    "select sum(quantity * price) as total_price from test group by customerId"
  ) {
    runAnalyzer(
      schema,
      "select sum(quantity * price) as total_price from test group by customerId"
    );
  }

  test(
    "select c1 as c2, s1 as s2 from (select customerId as c1, storeId as s1 from test)"
  ) {
    runAnalyzer(
      schema,
      "select c1 as c2, s1 as s2 from (select customerId as c1, storeId as s1 from test)"
    );
  }

  test(
    "select customerId, storeId from test where price>0"
  ) {
    runAnalyzer(
      schema,
      "select customerId, storeId from test where price>0"
    );
  }
}
