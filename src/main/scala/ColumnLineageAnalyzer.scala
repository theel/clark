import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.Alias
import java.util.logging.Logger
import org.apache.log4j
case class Dependency(
    attrName: String,
    exp: Expression,
    typeName: String
)

case class ResolvedDependency(
    val attrName: String,
    val children: List[ResolvedDependency],
    val reason: Dependency
) {

  override def toString() = {
    val sb = new StringBuffer();
    dumpString("  ", sb)
    sb.toString()
  }

  def dumpString(prefix: String, sb: StringBuffer): Unit = {
    sb.append(prefix + "+-- " + attrName)
    if (reason != null) { sb.append(" (" + reason.typeName + ")") }
    sb.append("\n")
    children.map(_.dumpString(prefix + "  ", sb))
  }
}
case class TableDef(schema: StructType, name: String)

class ColumnLineageAnalyzer(sources: List[TableDef]) {
  val logger = log4j.Logger.getLogger(classOf[ColumnLineageAnalyzer])

  val columnToParents =
    scala.collection.mutable.Map[String, List[
      Dependency
    ]]() //we temporary cache dependency because the analysis

  var logicalPlan: LogicalPlan = null

  def analyze(sql: String) {
    val spark = SparkSession.builder
      .appName("Clark")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    sources.foreach { s =>
      val source =
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], s.schema)
      val view = source.createOrReplaceTempView(s.name);
    }
    val df = spark.sql(sql)
    logicalPlan = df.queryExecution.analyzed
    logger.info("Analyzing plan\n" + logicalPlan)
    analyzePlan(logicalPlan)
  }

  def getOutputColumnLineage(): List[ResolvedDependency] = {
    logicalPlan.output.map { e =>
      ResolvedDependency(
        e.name,
        List(resolveDependency(e.toString, null)),
        null
      )
    }.toList
  }

  private def resolveDependency(
      attrName: String,
      reason: Dependency
  ): ResolvedDependency = {
    columnToParents.get(attrName) match {
      case None =>
        throw new IllegalStateException(
          "Unable to resolve dependency for " + attrName
        )
      case Some(deps) =>
        ResolvedDependency(
          attrName,
          deps.map(d =>
            if (d.typeName == "input") {
              ResolvedDependency(d.attrName, List(), d)
            } else { //need recursion
              resolveDependency(d.attrName, d)
            }
          ),
          reason
        )
    }
  }

  private def analyzePlan(plan: LogicalPlan): List[Dependency] = {
    logger.debug("Analyzing " + plan)
    plan match {
      case Aggregate(groupingExpression, aggregateExpression, child) => {
        //Step1: gather info from child first
        val deps = analyzePlan(child);

        //Step 2: now, analyze aggregateExpression for lineage and store in columnsToParents
        val groupDependency = groupingExpression.flatMap { ex =>
          getDependency(ex, ex, "groupBy")
        }
        aggregateExpression.zip(plan.output).map { case (aggExp, output) =>
          columnToParents.put(
            output.toString(),
            getDependency(
              aggExp,
              aggExp,
              "lineage"
            ) ++ groupDependency ++ deps
          );
        }
      }
      case Project(expr, child) => {

        val deps = analyzePlan(child)
        plan.output.zip(expr).foreach { case (out, e) =>
          if (!columnToParents.contains(out.toString())) { //sometimes project with subquery just echo LogicalRDD
            columnToParents
              .put(out.toString(), getDependency(e, e, "lineage") ++ deps)
          } else {
            val existing = columnToParents.get(out.toString)
            columnToParents
              .put(out.toString(), existing.get ++ deps)
          }
        }
      }
      case Filter(expr, child) => {
        val deps = analyzePlan(child)
        assert(deps.length == 0)
        return getDependency(
          expr,
          expr,
          "condition"
        ) //only filter return dependencies
      }
      case SubqueryAlias(id, child) =>
        analyzePlan(child)
      case LogicalRDD(
            output,
            rdd,
            outputPartitioning,
            outputOrdering,
            isStreaming
          ) => //map it to source fields
        plan.output.foreach { attr =>
          {
            val dependency = Dependency(attr.name, null, "input")
            columnToParents.put(attr.toString(), List(dependency))
          }
        }
    }
    List() //default return nothing
  }

  //any expression can become a list of dependencies
  private def getDependency(
      expr: Expression,
      root: Expression,
      depType: String
  ): List[Dependency] = {
    expr match {
      case attr: Attribute => List(Dependency(attr.toString, root, depType))
      case other =>
        other.children.flatMap { e =>
          getDependency(e, root, depType)
        }.toList
    }
  }

}

object ColumnLineageAnalyzer {
  def main(args: List[String]) {
    if (args.length < 3) {
      println(
        s"Usage: ${getClass.getSimpleName} <schemaJson> <tableName> <sql>"
      )
    } else {
      val (schemaJson, name, sql) = (args(0), args(1), args(2))
      val schema = DataType.fromJson(args(0)).asInstanceOf[StructType]
      val analyzer = new ColumnLineageAnalyzer(List(new TableDef(schema, name)))
      analyzer.analyze(sql)
    }
  }
}
