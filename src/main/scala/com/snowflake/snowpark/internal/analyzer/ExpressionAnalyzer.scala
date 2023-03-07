package com.snowflake.snowpark.internal.analyzer

import scala.collection.mutable.{Map => MMap}

private[snowpark] object ExpressionAnalyzer {
  def apply(aliasMap: Map[ExprId, String]): ExpressionAnalyzer =
    new ExpressionAnalyzer(aliasMap)

  def apply(): ExpressionAnalyzer =
    new ExpressionAnalyzer(Map.empty)

  // create new analyzer by combining two alias maps
  def apply(map1: Map[ExprId, String], map2: Map[ExprId, String]): ExpressionAnalyzer = {
    val common = map1.keySet & map2.keySet
    val result = (map1 ++ map2).filter {
      // remove common column, let (df1.join(df2))
      // .join(df2.join(df3)).select(df2) report error
      case (id, _) => !common.contains(id)
    }
    new ExpressionAnalyzer(result)
  }

  def apply(maps: Seq[Map[ExprId, String]]): ExpressionAnalyzer = {
    maps.foldLeft(ExpressionAnalyzer()) {
      case (expAnalyzer, map) => ExpressionAnalyzer(expAnalyzer.getAliasMap, map)
    }
  }
}

private[snowpark] class ExpressionAnalyzer(aliasMap: Map[ExprId, String]) {
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
