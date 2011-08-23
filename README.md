Hoodie
======

This is an experimental fun project that implements nearest neighbour search using in-memory index structures
suitable for graph databases

Currently the only provided implementation uses in-memory skip-lists

Hoodie stores an arbitrary set of records where each record is a multidimensional value. The list of fields
of each record is described statically by creating a "Schema" using the chosen implementation's schema
factory.

Example.

      val builder = schemaFactory.newBuilder
      builder.addField[Int]("age")
      builder.addField[Boolean]("bald")
      builder.addField[Float]("size")
      val schema = builder.result

The resulting schema instance provides insert, delete, and search calls for nearest neighbour search.

