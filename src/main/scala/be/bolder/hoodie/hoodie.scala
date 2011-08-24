package be.bolder

import scala.collection.mutable.Builder

package object hoodie {

  // Vector of weights
  // TODO: Length checking in implementation
  type Weighting = IndexedSeq[Float]

  // "Weighted distance measure for values of type T"
  // (like Ordering[T], just using a float based compare and separate equality check due to precision problems)
  trait WDM[T] {
    def eq(a: T, b: T): Boolean

    def compare(a: T, b: T): Float

    def distance(w: Float, a: T, b: T): Float = math.abs(compare(a, b)) * math.abs(w)

    def lt(a: T, b: T) = compare(a,b) < 0.0f
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
import util.Random
import com.sun.xml.internal.ws.developer.MemberSubmissionAddressing.Validation
import java.net.DatagramSocketImpl

// Stock symmetric WDMs for primitive types
object PlainWDM {
  implicit object intWDM extends WDM[Int] {
    def compare(a: Int, b: Int): Float = (a-b).toFloat
    def eq(a: Int, b: Int): Boolean = a == b
  }

  implicit object floatWDM extends WDM[Float] {
    def compare(a: Float, b: Float): Float = (a-b).toFloat
    def eq(a: Float, b: Float): Boolean = a == b
  }


  implicit object boolWDM extends WDM[Boolean] {
    def compare(a: Boolean, b: Boolean): Float = if (a) (if (b) 0.0f else 1.0f) else (if (b) -1.0f else 0.0f)
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

  type F[T] <: Field[R, T]

  def get(record: R): T
  def set(record: R, value: T)

  def parse = ixs._1
  def show = ixs._2

  def getAsString(record: R): String = {
    val value = get(record)
    show(value)
  }

  def setFromString(record: R, str: String) {
    set(record, parse(str))
  }

  // *Field* distance defined for records
  def distance(w: Float, a:R, b: R): Float = wdm.distance(w, get(a), get(b))


  def retypeField[S](mf2: Manifest[S]): Option[F[S]] =
    if (mf <:< mf2) Some(this.asInstanceOf[F[S]]) else None
}


// Data schema for records of type R
abstract class Schema[R] {
  var separator = ","

  type F[T] <: Field[R, T]

  val fields: IndexedSeq[F[_]]

  def distance(weights: Weighting, a:R, b: R): Float =
    // yuck... but still better to use float based squaring with large number of fields
    math.sqrt(distance(weights, a, b).toDouble).toFloat


  def distanceSquare(weights: Weighting, a:R, b: R): Float = {
    var dist = 0.0f
    for (i <- 0 until fields.length) {
      val field  = fields(i)
      val weight = weights(i)
      val fieldDist = field.distance(weight, a, b)
      dist += (fieldDist * fieldDist)
    }
    dist
  }

  def getAsString(record: R): String = fields.map( _.getAsString(record) ).mkString(separator)

  def setFromString(record: R, lineStr: String) {
    for ( (field, valueStr) <- fields.zip(lineStr.split(separator).map( _.trim() )) )
      field.setFromString(record, valueStr)
  }


  // Create an empty, unpopulated record
  def mkRecord: R

  // Creates a record by parsing lineStr according to the schema
  // (The resulting record is NOT automatically inserted into the index structure)
  def mkFromString(lineStr: String): R

  // Insert record into index structure
  def insert(record: R)


  // Retrieve nearest neighbors of record using the given weighting
  def search(weights: Weighting, query: R)(cont: Option[(Float, R)] => Boolean)
}


// Factory for creating schemas of type R
abstract class SchemaFactory {
  type R
  type F[T] <: Field[R, T]
  type B <: Builder

  abstract class Builder {
    // rewind builder
    def clear()

    // add fields
    def addField[T](name: String)(implicit ixs: IXS[T], wdm: WDM[T], mf: Manifest[T]): F[T]

    // construct schema
    def result: Schema[R]
  }

  def newBuilder: B
}

}



