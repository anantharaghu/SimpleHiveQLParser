package org.arp.DataFlowViewer

import java.io.File
import java.sql.{Connection, DriverManager}

object FlowGraphParser {
  val sourceTablePattern = "FROM|JOIN"
  val insertTablePattern = "INSERT(\\s+OVERWRITE|\\s+INTO)\\s+TABLE"
  val createTablePattern = "CREATE\\s+TABLE(\\s+IF\\s+NOT\\s+EXISTS)?"
  val patterns = s"(?i)($createTablePattern|$insertTablePattern|$sourceTablePattern)\\s+\\S+\\w+".r
  val graph = new FlowGraph

  def main(args: Array[String]) {
    //val lines = "create table xyz from schema.table inner join  sch1.table1  where; insert into table xyz (select * from  sch1.table1);".toUpperCase()
    val queryFolder = "\\\\dwshome-a.homeoffice.wal-mart.com\\dwsuserdata$\\anana1\\Documents\\assortment_analytics\\ao_base_data_layer\\oozie\\load_base_data_layer"
    val files = new java.io.File(queryFolder).listFiles
    files.foreach(parseQueries(_))
    println(patterns)
  }

  def parseQueries(fileName: File) {
    val lines = scala.io.Source.fromFile(fileName).getLines().mkString(" ")
    for (query <- lines.split(";")) {
      var node: Node = null
      for (str <- patterns.findAllIn(query)) {
        val clause = str.trim
        println(s"Clause: $clause")
        if (clause.matches(s"\\A($createTablePattern)\\s+.*")) {
          val tableName = clause.substring(clause.lastIndexOf(" "))
          node = new Node(tableName.trim)
          graph.addNode(new Node(tableName.trim))
          println("Added Node : " + tableName.trim)
        }
        if (clause.matches(s"\\A($insertTablePattern)\\s+.*")) {
          val tableName = clause.substring(clause.lastIndexOf(" "))
          node = new Node(tableName.trim)
        }
        if (clause.matches(s"\\A($sourceTablePattern)\\s+.*")) {
          val tableName = clause.substring(clause.lastIndexOf(" "))
          graph.addSource(node, tableName.trim)
        }
        println(s"$clause ")
      }
      //println("------")
    }
    println(graph.toString)
    graph.persistGraph
  }
}