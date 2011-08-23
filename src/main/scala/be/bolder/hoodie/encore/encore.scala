package be.bolder.hoodie.encore

import be.bolder.hoodie._
import scala.reflect.Manifest
import collection.mutable.{BitSet}
import collection.immutable.Queue

// Pure in-memory implementation of the algorithm
object EncoreSchemaFactory extends SchemaFactory {
  override type R = PrimRecord
  override type B = PrimBuilder
  override type F[T] = B#PrimField[T]

  // Static state used during schema building
  final class State {
    private[EncoreSchemaFactory] var fieldCount: Int = 0
    private[EncoreSchemaFactory] var intFieldCount: Int = 0
    private[EncoreSchemaFactory] var boolFieldCount: Int = 0
    private[EncoreSchemaFactory] var floatFieldCount: Int = 0
  }

  // PrimRecords aggregate field values by type into arrays
  final class PrimRecord(state: EncoreSchemaFactory.State) {
    // regular int field values + stretch information from skip list left pointers
    private[EncoreSchemaFactory] val intMap: Array[Int] = new Array[Int]( state.intFieldCount + state.fieldCount )
    private[EncoreSchemaFactory] val boolMap: BitSet = new BitSet( state.boolFieldCount )
    private[EncoreSchemaFactory] val floatMap: Array[Float] = new Array[Float]( state.floatFieldCount )

    // skip list pointers  (left, down)
    private[EncoreSchemaFactory] val ptrMap: Array[PrimRecord] = new Array[PrimRecord]( state.fieldCount * 2 )
  }

  // PrimBuilder ties it all together; as fields get added their typeIndex is set
  final class PrimBuilder extends Builder {
    private var state = new State
    private var fields = Queue.newBuilder[F[_]]

    // PrimFields keep track of a type index to store values into corresponding PrimRecord array slots
    class PrimField[T](aName: String, val state: State)(implicit anIxs: IXS[T], aWdm: WDM[T], aMf: Manifest[T])
      extends Field[R, T](aName, anIxs, aWdm, aMf) {

      val index = state.fieldCount

      val typeIndex = mf match {
        case Manifest.Int => state.intFieldCount
        case Manifest.Boolean => state.boolFieldCount
        case Manifest.Float => state.floatFieldCount
        case _ => throw new IllegalArgumentException("Unsupported field type")
      }

      def get(record: R): T =
        mf match {
          case Manifest.Int => record.intMap(typeIndex).asInstanceOf[T]
          case Manifest.Boolean => record.boolMap(typeIndex).asInstanceOf[T]
          case Manifest.Float => record.floatMap(typeIndex).asInstanceOf[T]
        }

      def set(record: R, value: T) {
        mf match {
          case Manifest.Int => record.intMap(typeIndex) = value.asInstanceOf[Int]
          case Manifest.Boolean => record.boolMap(typeIndex) = value.asInstanceOf[Boolean]
          case Manifest.Float => record.floatMap(typeIndex) = value.asInstanceOf[Float]
        }
      }

      private[EncoreSchemaFactory] def getRight(record: R): R = record.ptrMap(index * 2)
      private[EncoreSchemaFactory] def setRight(record: R, value: R) { record.ptrMap(index * 2) = value }

      private[EncoreSchemaFactory] def getDown(record: R): R = record.ptrMap((index * 2) + 1)
      private[EncoreSchemaFactory] def setDown(record: R, value: R) { record.ptrMap((index * 2) + 1) = value}

      private[EncoreSchemaFactory] def getStretch(record: R): Int = record.intMap(state.fieldCount + index)
      private[EncoreSchemaFactory] def setStretch(record: R, value: Int) {
        record.intMap(state.fieldCount + index) = value
      }
    }

    // rewind builder
    def clear() {
      state = new State
      fields = Queue.newBuilder[F[_]]
    }

    // add fields
    def addField[T](name: String)(implicit ixs: IXS[T], wdm: WDM[T], mf: Manifest[T]): F[T] = {
      var res: F[T] = null
      mf match {
        case Manifest.Int =>
          res = new PrimField(name, state); state.intFieldCount += 1
        case Manifest.Boolean =>
          res = new PrimField(name, state); state.boolFieldCount += 1
        case Manifest.Float =>
          res = new PrimField(name, state); state.floatFieldCount += 1
        case _ => throw new IllegalArgumentException("Unsupported field type")
      }
      state.fieldCount += 1
      fields += res
      res
    }

    // construct schema
    def result: Schema[R] = {
      val res = new PrimSchema(fields.result().toIndexedSeq, state)
      clear()
      res
    }
  }

  def newBuilder: B = new PrimBuilder

  final class PrimSchema(val fields: IndexedSeq[F[_]], val state: State) extends Schema[PrimRecord] {
    override type F[T] = EncoreSchemaFactory.this.F[T]

    def mkRecord: PrimRecord = new PrimRecord(state)

    def mkFromString(lineStr: String): PrimRecord = {
      val record  = mkRecord
      setFromString(record, lineStr)
      record
    }

    // Insert record into index structure
    def insert(record: PrimRecord) {

    }

    // Retrieve nearest neighbors of record using the given weighting
    def search(weights: Weighting, record: PrimRecord, cont: Option[(Float, PrimRecord)] => Boolean) {

    }
  }

}

