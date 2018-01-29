package org.arp.DataFlowViewer

import scala.collection.mutable.MutableList

class FlowGraph {

  var graph = MutableList[Node]()

  def addNode(node : Node) {
    graph += node
  }

  def addSource(node : Node, source : Node) {
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

  def addSource(node : Node, source : String) {
    addSource(node, graph.find(x => x.name.equalsIgnoreCase(source)).getOrElse(new Node(source)))
  }

  def getNodes : MutableList[Node] = {
    graph
  }
  override def toString : String = {
    graph.mkString(", ")
  }
}