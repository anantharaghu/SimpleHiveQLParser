package org.arp.DataFlowViewer

import scala.collection.mutable.MutableList

class Node (var name : String) {

  var sources = MutableList[Node]()

  var children = MutableList[Node]()

  def addSource(node : Node) {
    if (!sources.contains(node))
      sources += node
    node.addChild(this)
  }

  private def addChild(node : Node) {
    if (!children.contains(node))
      children += node
  }

  override def toString() : String = {
    var str = s"( Node : $name - Sources : "
    for (src <- sources) str += src.name + ", "
    str += " - Children : "
    for (child <- children) str += child.name + ", "
    s"$str )"
  }

  override def equals(otherNode : Any) : Boolean = {
    otherNode match {
      case otherNode : Node => this.name.equalsIgnoreCase(otherNode.name)
      case _ => false
    }

  }
}