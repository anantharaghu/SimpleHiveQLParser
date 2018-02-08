package org.arp.DataFlowViewer

import java.sql.{DriverManager, Statement}

import scala.collection.mutable.MutableList

class FlowGraph {

  private val graph = MutableList[Node]()

  def addSource(node: Node, source: String) {
    val sourceNode = new Node(source)
    val srcNode = graph.find(_.name.equalsIgnoreCase(source)).getOrElse(sourceNode)
    addSource(node, srcNode)
  }

  def addSource(node: Node, source: Node) {
    addNode(node)
    addNode(source)
    node.addSource(source)
  }

  def addNode(node: String): Unit = {
    addNode(new Node(node.trim))
  }

  def addNode(node: Node) {
    if (!graph.contains(node)) {
      graph += node
      println(s"Added node ${node.name}")
    }
  }

  def findNode(node: String): Option[Node] = {
    graph.find(_.name.equalsIgnoreCase(node))
  }

  def getNodes: MutableList[Node] = {
    graph
  }

  override def toString: String = {
    graph.mkString(", ")
  }

  def persistGraph: Unit = {
    Class.forName("org.h2.Driver")
    val connection = DriverManager.getConnection("jdbc:h2:tcp://localhost/~/test", "sa", "sc")
    val stmt: Statement = connection.createStatement
    try {
      graph.foreach(node => {
        if (node.sources == null || node.sources.isEmpty)
          stmt.execute(s"insert into FLOWGRAPH.NODES values ('${node.name}','')")
        else {
          node.sources.foreach(source => {
            println(s"Inserting $node")
            val insertQuery = s"insert into FLOWGRAPH.NODES values ('${node.name}','${source.name}')"
            val insertStat = stmt.execute(insertQuery)
            println(insertStat)
          })
        }
      })
      connection commit
    } catch {
      case e: Exception => connection.rollback
    }
    finally {
      stmt.close
      connection.close()
    }
  }
}