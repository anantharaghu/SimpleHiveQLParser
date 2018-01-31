package org.arp.DataFlowViewer

object FlowGraphParser {

    def main (args : Array[String]) {
      //val lines = "create table xyz from schema.table inner join  sch1.table1  where; insert into table xyz (select * from  sch1.table1);".toUpperCase()
      val queryFile = "/Users/Raghu/test.sql"
      val lines = scala.io.Source.fromFile(queryFile).mkString(" ")
      val sourceTablePattern = "FROM|JOIN"
      val insertTablePattern = "INSERT(\\s+OVERWRITE|\\s+INTO)\\s+TABLE"
      val createTablePattern = "CREATE\\s+TABLE(\\s+IF\\s+NOT\\s+EXISTS)?"
      val patterns = s"(?i)($createTablePattern|$insertTablePattern|$sourceTablePattern)\\s+\\S+\\w+".r
      println(patterns)
      val graph = new FlowGraph

      for (query <- lines.split(";")) {
        var node : Node = null
        for (str <- patterns.findAllIn(query)){
          if (str.matches(s"\\A($createTablePattern)\\s+.*")) {
            val tableName = str.substring(str.lastIndexOf(" "))
            node = new Node(tableName.trim)
            graph.addNode(new Node(tableName.trim))
            println("Added Node : " + tableName.trim)
          }
          if (str.matches(s"\\A($insertTablePattern)\\s+.*")) {
            val tableName = str.substring(str.lastIndexOf(" "))
            node = new Node(tableName.trim)
          }
          if (str.matches(s"\\A($sourceTablePattern)\\s+.*")) {
            val tableName = str.substring(str.lastIndexOf(" "))
            graph.addSource(node, tableName.trim)
          }
          println(s"$str ")
        }
        println("------")
      }

      println(graph.toString)
    }

  }