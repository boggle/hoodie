package be.bolder.hoodie.blue

import com.tinkerpop.blueprints.pgm.{Vertex, Graph}
import be.bolder.hoodie.Field

class SkipListHelper[T](val graph: Graph, val field: Field[Vertex, T]) {

}