package be.bolder.hoodie.blue

import be.bolder.hoodie._
import com.tinkerpop.blueprints.pgm.{Vertex, Graph}

// SchemaFactory for blueprint graph nodes
class BlueSchemaFactory[G <: Graph](val graph: G) extends SchemaFactory[Vertex] {

  def mkField[T](name: String)(implicit ixs: IXS[T], wdm: WDM[T], mf: Manifest[T]): Field[Vertex, T] =
    new Field[Vertex, T](name, ixs, wdm, mf) {
      val protectedName = "_" + name

      def get(record: Vertex): T = record.getProperty(protectedName).asInstanceOf[T]

      def set(record: Vertex, value: T) {
        record.setProperty(protectedName, value)
      }
    }

  def mkSchema(fields: Field[Vertex, _]*): Schema[Vertex] = {
    val schemaFields = fields.toIndexedSeq

    new Schema[Vertex] {
      override val fields = schemaFields

      def mkFromString(lineStr: String): Vertex = {
        val record  = graph.addVertex(null)
        setFromString(record, lineStr)
        record
      }

      // Insert record into index structure
      def insert(record: Vertex) {

      }

      // Retrieve nearest neighbors of record using the given weighting
      def search(weights: Weighting, record: Vertex, cont: Option[(Float, Vertex)] => Boolean) {

      }


    }
  }
}


