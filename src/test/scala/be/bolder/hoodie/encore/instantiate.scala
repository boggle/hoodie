package be.bolder.hoodie.encore

import java.lang.Runtime
import be.bolder.hoodie.encore.EncoreSchemaFactory.PrimRecord

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

      val rec1 = schema.mkFromString("12, true, 4.5")
      val rec2 = schema.mkFromString("4, false, 2.5")
      val rec3 = schema.mkFromString("12, false, 8.5")
      val rec4 = schema.mkFromString("18, true, 23")

      schema.insert(rec1)
      schema.insert(rec2)
      schema.insert(rec3)
      schema.insert(rec4)
    }
  }
}

object EncoreIndexes {
  def main(args: Array[String]) {
      val schemaFactory = EncoreSchemaFactory

      {
        import be.bolder.hoodie.PlainIXS._
        import be.bolder.hoodie.PlainWDM._

        val builder = schemaFactory.newBuilder
        val field_x = builder.addField[Int]("x")
        val field_y = builder.addField[Int]("y")
        val field_z = builder.addField[Int]("z")
        val field_t = builder.addField[Int]("t")
        val field_v = builder.addField[Int]("v")
        val schema = builder.result

        val len = 10

        val runtime = Runtime.getRuntime()
        val memBefore = runtime.freeMemory()

        for (x <- 0.until(len))
          for (y <- 0.until(len))
            for (z <- 0.until(len))
              for (t <- 0.until(len))
                for (v <- 0.until(len)) {
                  val record = schema.mkRecord
                  field_x.set(record, x)
                  field_y.set(record, y)
                  field_z.set(record, z)
                  field_t.set(record, t)
                  field_v.set(record, v)
                  schema.insert(record)
                }

        val numRecs = len*len*len*len*len
        val memAfter = runtime.freeMemory()
        System.out.println( (memBefore-memAfter) / numRecs )

        val record = schema.mkRecord
        System.out.println(schema.getAsString(field_x.search(4)))
        System.out.println(schema.getAsString(field_y.search(6)))
        System.out.println(schema.getAsString(field_z.search(7)))
        System.out.println(schema.getAsString(field_t.search(3)))
        System.out.println(schema.getAsString(field_v.search(8)))
      }
  }
}

object EncoreIndexes2 {
  def main(args: Array[String]) {
      val schemaFactory = EncoreSchemaFactory

      {
        import be.bolder.hoodie.PlainIXS._
        import be.bolder.hoodie.PlainWDM._

        val builder = schemaFactory.newBuilder
        val field_x = builder.addField[Int]("x")
        val field_y = builder.addField[Int]("y")
        val field_z = builder.addField[Int]("z")
        val schema = builder.result

        val len = 3

        for (x <- 0.until(len))
        for (y <- 0.until(len))
        for (z <- 0.until(len))
        {
            val record = schema.mkRecord
            field_x.set(record, x)
            field_y.set(record, y)
            field_z.set(record, z)
            schema.insert(record)
        }

        val numRecs = len*len*len

        var count = 0

        val fst = field_x.head.get
        var lst = fst
        System.out.println("fst: " + fst)


        System.out.println("1 >>>>")
        for (value <- field_x.succIterator(1.0f, fst)) {
          lst = value._2
          System.out.println(value + " = " + field_x.get(value._2))
          count += 1
        }
        System.out.println(numRecs)
        System.out.println(count)


        System.out.println("lst: " + lst)
        System.out.println("2 >>>>")
        count = 0
        for (value <- field_x.predIterator(1.0f, lst)) {
          System.out.println(value + " = " + field_x.get(value._2))
          count += 1
        }
        System.out.println(numRecs)
        System.out.println(count)

        System.out.println("3 >>>>")
        count = 0
        for (value <- field_x.iterator(1.0f, field_x.search(1))) {
          System.out.println(value + " = " + field_x.get(value._2))
          count += 1
        }
        System.out.println(numRecs)
        System.out.println(count)

        System.out.println("4 >>>>")
        count = 0
        for (value <- field_z.iterator(1.0f, field_z.search(1))) {
          System.out.println(value + " = " + field_z.get(value._2))
          count += 1
        }
        System.out.println(numRecs)
        System.out.println(count)

        val start = field_x.search(2)
        System.out.println("start: " + start + " = " + field_z.get(start))
        System.out.println("5 >>>>")
        count = 0
        for (value <- field_z.iterator(1.0f, start)) {
          System.out.println(value + " = " + field_z.get(value._2))
          count += 1
        }
        System.out.println(numRecs)
        System.out.println(count)
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