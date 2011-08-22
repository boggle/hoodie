package be.bolder.hoodie.blue

import com.tinkerpop.blueprints.pgm.impls.tg.TinkerGraphFactory
import com.tinkerpop.blueprints.pgm.Graph

object BlueInstantiates {

  def main(args: Array[String]) {
    val graph = TinkerGraphFactory.createTinkerGraph()
    val schemaFactory = new BlueSchemaFactory[Graph](graph)

    {
      import schemaFactory.mkField
      import be.bolder.hoodie.PlainIXS._
      import be.bolder.hoodie.PlainWDM._

      val schema = schemaFactory.mkSchema(mkField[Int]("age"), mkField[Boolean]("bald"), mkField[Float]("size"))
    }
  }
}