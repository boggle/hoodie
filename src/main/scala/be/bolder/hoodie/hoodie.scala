package be.bolder

package object hoodie {

  // Vector of weights
  // TODO: Length checking in implementation
  type Weighting = IndexedSeq[Float]

  // "Weighted distance measure for values of type T"
  // (like Ordering[T], just using a float based compare and separate equality check)
  trait WDM[T] {
    def compare(a: T, b: T): Float
    def eq(a: T, b: T): Boolean

    def distance(w: Float, a: T, b: T): Float = math.abs(compare(a, b)) * math.abs(w)

    def neq(a: T, b: T): Boolean = !eq(a, b)

    def lt(a: T, b: T) = !eq(a, b) && compare(a,b) < 0
    def gt(a: T, b: T) = !eq(a, b) && compare(a,b) > 0

    def leq(a: T, b: T) = eq(a, b) || compare(a,b) < 0
    def geq(a: T, b: T) = eq(a, b) || compare(a,b) > 0
  }


  // Parse and format functions for  in/externalizing values of type T using Strings
  // "Internalize/Externalize as String"
  //
  // This is mainly used for importing data from CSV files
  //
  type IXS[T] = (String => T, T => String)
}

package hoodie {

import reflect.Manifest
import com.tinkerpop.blueprints.pgm.{Vertex, Graph}
import util.Random

// Stock symmetric WDMs for primitive types
object PlainWDM {
  implicit object intWDM extends WDM[Int] {
    def compare(a: Int, b: Int): Float = (b-a).toFloat
    def eq(a: Int, b: Int): Boolean = a == b
  }

  implicit object floatWDM extends WDM[Float] {
    def compare(a: Float, b: Float): Float = (b-a).toFloat
    def eq(a: Float, b: Float): Boolean = a == b
  }


  implicit object boolWDM extends WDM[Boolean] {
    def compare(a: Boolean, b: Boolean): Float = if (b) (if (a) 0.0f else 1.0f) else (if (a) -1.0f else 0.0f)
    def eq(a: Boolean, b: Boolean): Boolean = a && b
  }
}


// Stock IXSs for primitive types
object PlainIXS {
  implicit def intIXS: IXS[Int] = ( java.lang.Integer.parseInt, _.toString() )
  implicit def floatIXS: IXS[Float] = ( java.lang.Float.parseFloat, _.toString() )
  implicit def boolIXS: IXS[Boolean] = ( (str: String) => str.toLowerCase == "true", _.toString )
}


// Named, typed fields for record objects of type R
//
// The idea behind this is to abstract from field type in a way that makes it possible to avoid boxing overhead
// (doesn't work with blueprints but might with neo4j)
abstract class Field[R, T](val name: String,
                           implicit val ixs: IXS[T], implicit val wdm: WDM[T], implicit val mf: Manifest[T]) {
  def get(record: R): T
  def set(record: R, value: T)

  def parse = ixs._1
  def show = ixs._2

  def getAsString(record: R): String = show(get(record))

  def setFromString(record: R, str: String) {
    set(record, parse(str))
  }

  // Field distance defined for records (This is a WDM[R])
  def distance(w: Float, a:R, b: R): Float = wdm.distance(w, get(a), get(b))
}


// Data schema for records of type R
trait Schema[R] {
  var separator = ","

  val fields: IndexedSeq[Field[R, _]]

  def distance(weights: Weighting, a:R, b: R): Float = {
    var dist = 0.0f
    for (i <- 0 until fields.length) {
      val field  = fields(i)
      val weight = weights(i)
      dist += field.distance(weight, a, b)
    }
    dist
  }

  def getAsString(record: R): String = fields.map( _.getAsString(record) ).mkString(separator)

  def setFromString(record: R, lineStr: String) {
    for ( (field, valueStr) <- fields.zip(lineStr.split(separator).map( _.trim() )) )
      field.setFromString(record, valueStr)
  }

  // Creates a record by parsing lineStr according to the schema
  // (The resulting record is NOT automatically inserted into the index structure)
  def mkFromString(lineStr: String): R

  // Insert record into index structure
  def insert(record: R)


  // Retrieve nearest neighbors of record using the given weighting
  def search(weights: Weighting, record: R, cont: Option[(Float, R)] => Boolean)
}


// Factory for creating schemas of type R
trait SchemaFactory[R] {
  // Create schema for storing records with the given fields
  def mkSchema(fields: Field[R, _]*): Schema[R]

  // Create field descriptor
  def mkField[T <: Ordered[T]](name: String)(implicit ixs: IXS[T], wdm: WDM[T], mf: Manifest[T]): Field[R, T]
}


}



