package com.kakao.cuesheet.deps

import com.kakao.mango.logging.Logging

import scala.collection.mutable

class DependencyGraph(val nodes: Seq[DependencyNode]) extends Logging {
  val lookup = new mutable.LinkedHashMap[ManagedDependency, ManagedDependencyNode]
  val unmanaged = new mutable.LinkedHashMap[String, UnmanagedDependencyNode]

  nodes.foreach {
    case node: ManagedDependencyNode =>
      val dep = ManagedDependency(node.group, node.artifact, node.classifier)
      if (!lookup.contains(dep)) {
        lookup.put(dep, node)
      } else {
        logger.warn(s"Duplicate dependency $dep: ${lookup(dep)} and $node")
      }
    case node: UnmanagedDependencyNode =>
      unmanaged.put(node.path, node)
    case node: DirectoryDependencyNode =>
      val zipped = node.compressed
      unmanaged.put(zipped.path, zipped)
    case node: JavaRuntimeDependencyNode =>
      // safe to ignore
  }

  def getChildren(root: ManagedDependency): Seq[ManagedDependency] = {
    if (!lookup.contains(root)) {
      throw new IllegalArgumentException(s"Dependency not found: $root")
    }

    lookup(root).children
  }

  /** divide the graph into all transitive dependencies of given root, and all others
    *
    * @param roots    The root dependencies to search its transitive depenencies
    * @return         A tuple containing a sequence of the transitive dependencies and all other nodes
    */
  def divide(root: ManagedDependency, roots: ManagedDependency*): (Seq[ManagedDependencyNode], Seq[DependencyNode]) = {
    val result = new mutable.LinkedHashMap[ManagedDependency, ManagedDependencyNode]

    def traverse(node: ManagedDependencyNode): Unit = {
      result.put(node.key, node)
      node.children.foreach { child =>
        if (!result.contains(child) && lookup.contains(child)) {
          val childNode = lookup(child)
          traverse(childNode)
        }
      }
    }

    (root +: roots).foreach { root =>
      if (!lookup.contains(root)) {
        throw new IllegalArgumentException(s"Dependency not found: $root")
      }
      traverse(lookup(root))
    }

    val transitiveNodes = result.values.toSeq
    val transitiveSet = transitiveNodes.toSet
    val others = lookup.values.filterNot(transitiveSet.contains) ++ unmanaged.values

    (transitiveNodes, others.toSeq)
  }

  /** Similar to the above, but getting all dependencies of a certain group ID
    *
    * @param group     all artifacts with this group ID will be considered as "roots"
    */
  def divide(group: String, groups: String*): (Seq[ManagedDependencyNode], Seq[DependencyNode]) = {
    val set = (group +: groups).toSet
    val roots = lookup.keys.filter(dep => set.contains(dep.group)).toSeq
    divide(roots.head, roots.tail: _*)
  }

  /** Similar to the above, but from either Apache Spark / CueSheet / Mango,
    * since this is isolating the fixed dependency and minimizing the application assembly is the main purpose this package
    */
  def divide(): (Seq[ManagedDependencyNode], Seq[DependencyNode]) = divide("org.apache.spark", "com.kakao.mango", "com.kakao.cuesheet")

}
