package org.arp.DataFlowViewer

import java.io.File

object FlowGraphParser {
  val sourceTablePattern = "FROM|JOIN"
  val insertTablePattern = "INSERT(\\s+OVERWRITE|\\s+INTO)\\s+TABLE"
  val createTablePattern = "CREATE\\s+TABLE(\\s+IF\\s+NOT\\s+EXISTS)?"
  val useSchemaPattern = "USE"
  val patterns = s"(?i)($createTablePattern|$insertTablePattern|$sourceTablePattern|$useSchemaPattern)\\s+\\S+".r
  val graph = new FlowGraph

  def main(args: Array[String]) {
    //val lines = "create table xyz from schema.table inner join  sch1.table1  where; insert into table xyz (select * from  sch1.table1);".toUpperCase()
    val queryFolder = "\\\\dwshome-a.homeoffice.wal-mart.com\\dwsuserdata$\\anana1\\Documents\\assortment_analytics\\ao_base_data_layer\\oozie\\load_base_data_layer"
    val files = new java.io.File(queryFolder).listFiles
    files.foreach(parseQueries(_))
    println(graph.toString)
    graph.persistGraph
  }

  def parseQueries(fileName: File) {
    val lines = scala.io.Source.fromFile(fileName).getLines().mkString(" ")
    for (query <- lines.split(";")) {
      var currentSchema: String = ""
      var node: Node = null
      for (str <- patterns.findAllIn(query)) {
        val clause = str.trim
        if (clause.matches(s"\\A($createTablePattern)\\s+.*") || clause.matches(s"\\A($insertTablePattern)\\s+.*")) {
          var tableName = clause.substring(clause.lastIndexOf(" ")).trim
          if (!tableName.contains("."))
            tableName = s"$currentSchema.$tableName"
          node = graph.findNode(tableName).getOrElse(new Node(tableName))
          graph.addNode(node)
        } else if (clause.matches(s"\\A($sourceTablePattern)\\s+.*")) {
          val tableName = clause.substring(clause.lastIndexOf(" ")).trim
          if (!tableName.startsWith("(")) {
            graph.addSource(node, tableName)
          }
        } else if (clause.matches(s"\\A($useSchemaPattern)\\s+.*")) {
          currentSchema = clause.substring(clause.lastIndexOf(" ")).trim
        }
      }
    }
  }

}