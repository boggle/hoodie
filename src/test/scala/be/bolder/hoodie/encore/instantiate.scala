package be.bolder.hoodie.encore

import java.lang.Runtime
import be.bolder.hoodie.encore.EncoreSchemaFactory.PrimRecord
import com.sun.tools.internal.ws.wsdl.document.Input

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

      val len = 4

      for (x <- 0.until(len))
      for (y <- 0.until(len))
      for (z <- 0.until(len))
      {
          val record = schema.mkRecord
          field_x.set(record, x)
          field_y.set(record, y)
          field_z.set(record, z)
          System.out.println(schema.getAsString(record))
          schema.insert(record)
      }

      val last = new collection.mutable.HashMap[String, (Float, PrimRecord)]
      for (field <- schema.fields) {
        System.out.println("Testing field " + field.name + " succ monotonicity")

        var count        = 0
        val fst          = field.head.get
        last(field.name) = (0.0f, fst)
        for (value <- field.succIterator(1.0f, fst)) {
          if (value._1 < last(field.name)._1)
            System.out.print("!!!!!!!! ")
          val pred = field.pred(value._2)
          System.out.println(value + " = " + schema.getAsString(value._2) + "; pred = " +
            (if (pred eq null) "null" else pred.toString))
          last(field.name) = value
          count += 1
        }
      }

      for (field <- schema.fields) {
        System.out.println("Testing field " + field.name + " pred monotonicity")

        var count = 0
        var prev  = (0.0f, last(field.name)._2)
        for (value <- field.predIterator(1.0f, prev._2)) {
          if (value._1 < prev._1)
            System.out.print("!!!!!!!! ")
          System.out.println(value + " = " + schema.getAsString(value._2))
          prev = value
          count += 1
        }
      }

      for (field <- schema.fields) {
        System.out.println("Testing field " + field.name + " join monotonicity")

        var count = 0
        val start: EncoreSchemaFactory.R = field.retypeField(scala.reflect.Manifest.Int).get.search(2)
        var prev  = (0.0f, start)
        for (value <- field.iterator(1.0f, prev._2)) {
          if (value._1 < prev._1)
            System.out.print("!!!!!!!! ")
          System.out.println(value + " = " + schema.getAsString(value._2))
          prev = value
          count += 1
        }
      }
    }
  }
}

object EncoreSearches {
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

      val len = 9

      var center: EncoreSchemaFactory.R = null

      for (x <- 0.until(len))
      for (y <- 0.until(len))
      for (z <- 0.until(len))
      {
          val record = schema.mkRecord
          field_x.set(record, x)
          field_y.set(record, y)
          field_z.set(record, z)
          if (x == 4 && y == 4 && z == 4)
            center = record
          schema.insert(record)
      }

      schema.search(Array.fill(schema.fields.length)(1.0f), center){ input => input match {
          case Some(value) => System.out.println(value + " = " + schema.getAsString(value._2))
          case None => System.out.println("Done.")
        }
        true
      }
    }
  }
}
