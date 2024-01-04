package com.snowflake.snowpark.internal.analyzer

import com.snowflake.snowpark.internal.ErrorMessage

import scala.collection.mutable.{Map => MMap}

private[snowpark] object ExpressionAnalyzer {
  def apply(aliasMap: Map[ExprId, String],
            dfAliasMap: Map[String, Seq[Attribute]]): ExpressionAnalyzer =
    new ExpressionAnalyzer(aliasMap, dfAliasMap)

  def apply(): ExpressionAnalyzer =
    new ExpressionAnalyzer(Map.empty, Map.empty)

  // create new analyzer by combining two alias maps
  def apply(map1: Map[ExprId, String], map2: Map[ExprId, String],
            dfAliasMap: Map[String, Seq[Attribute]]): ExpressionAnalyzer = {
    val common = map1.keySet & map2.keySet
    val result = (map1 ++ map2).filter {
      // remove common column, let (df1.join(df2))
      // .join(df2.join(df3)).select(df2) report error
      case (id, _) => !common.contains(id)
    }
    new ExpressionAnalyzer(result, dfAliasMap)
  }

  def apply(maps: Seq[Map[ExprId, String]],
            dfAliasMap: Map[String, Seq[Attribute]]): ExpressionAnalyzer = {
    maps.foldLeft(ExpressionAnalyzer()) {
      case (expAnalyzer, map) => ExpressionAnalyzer(expAnalyzer.getAliasMap, map, dfAliasMap)
    }
  }
}

private[snowpark] class ExpressionAnalyzer(aliasMap: Map[ExprId, String],
                                           dfAliasMap: Map[String, Seq[Attribute]]) {
  private val generatedAliasMap: MMap[ExprId, String] = MMap.empty

  def analyze(ex: Expression): Expression = ex match {
    case attr: Attribute =>
      val newName = aliasMap.getOrElse(attr.exprId, attr.name)
      Attribute(newName, attr.dataType, attr.nullable, attr.exprId)
    case Alias(child: Attribute, name, _) =>
      val quotedName = quoteName(name)
      generatedAliasMap += (child.exprId -> quotedName)
      aliasMap.filter(_._2 == child.name).foreach {
        case (id, _) => generatedAliasMap += (id -> quotedName)
      }
      if (quoteName(child.name) == quotedName) {
        // in case of renaming to the current name, we can't directly remove this alias,
        // and have to update the aliasMap. for example,
        // df.select(df("a").as("b")).select(df("a").as("a"))
        // if directly remove the second alias, the result will have column b but not a
        child
      } else {
        Alias(child, quotedName)
      }
    // removed useless alias
    case Alias(child: NamedExpression, name, _) if quoteName(child.name) == quoteName(name) =>
      child
    case UnresolvedDFAliasAttribute(name) =>
      val colNameSplit = name.split("\\.", 2)
      if (colNameSplit.length > 1 && dfAliasMap.contains(colNameSplit(0))) {
        val aliasOutput = dfAliasMap(colNameSplit(0))
        val aliasColName = colNameSplit(1)
        val normalizedColName = quoteName(aliasColName)
        val col = aliasOutput.filter(attr => attr.name.equals(normalizedColName))
        if (col.length == 1) {
          col.head.withName(normalizedColName)
        } else {
          throw ErrorMessage.DF_CANNOT_RESOLVE_COLUMN_NAME(aliasColName, aliasOutput.map(_.name))
        }
      } else {
        // if didn't find alias in the map
        name match {
          case "*" => Star(Seq.empty)
          case _ => UnresolvedAttribute(quoteName(name))
        }
      }
    case _ => ex
  }

  def getAliasMap: Map[ExprId, String] = {
    val result = MMap(aliasMap.toSeq: _*)
    generatedAliasMap.foreach {
      case (key, value) => result += (key -> value)
    }
    result.toMap
  }
}
