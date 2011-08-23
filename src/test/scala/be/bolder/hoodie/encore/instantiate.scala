package be.bolder.hoodie.encore

object EncoreInstantiates {
  def main(args: Array[String]) {
    val schemaFactory = EncoreSchemaFactory

    {
      import be.bolder.hoodie.PlainIXS._
      import be.bolder.hoodie.PlainWDM._

      val builder = schemaFactory.newBuilder
      builder.addField[Int]("age")
      builder.addField[Boolean]("bald")
      builder.addField[Float]("size")
      val schema = builder.result
    }
  }
}

/*
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

*/