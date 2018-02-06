package org.arp.DataFlowViewer

import java.sql.{Connection, DriverManager, Statement}

import scala.collection.mutable.MutableList

class FlowGraph {

  val graph = MutableList[Node]()

  def addNode(node: Node) {
    graph += node
  }

  def addSource(node: Node, source: Node) {
    if (!graph.contains(node)) {
      addNode(node)
    }
    if (source != null) {
      if (!graph.contains(source)) {
        addNode(source)
      }
      node.addSource(source)
    }
  }

  def addSource(node: Node, source: String) {
    addSource(node, graph.find(x => x.name.equalsIgnoreCase(source)).getOrElse(new Node(source)))
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
        node.sources.foreach(source => {
          println(s"Inserting $node")
          val insertQuery = s"insert into flowgraph.nodes values (${node.name},${source.name})"
          stmt.execute(insertQuery)
        })
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