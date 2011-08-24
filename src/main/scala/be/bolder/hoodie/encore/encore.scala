package be.bolder.hoodie.encore

import be.bolder.hoodie._
import scala.reflect.Manifest
import collection.mutable.{BitSet}
import collection.immutable.Queue
import java.lang.IllegalArgumentException
import runtime.FloatRef

// Nearest-neighbor search based on in-memory skip list
//
// See README.md for details
//
// @author Stefan Plantikow <stefan.plantikow@gogolemail.com>
//
object EncoreSchemaFactory extends SchemaFactory {
  // Type of records
  override type R = PrimRecord

  // Type of Schema Builder
  override type B = PrimBuilder

  // Type of Field objects used to access records
  override type F[T] = B#PrimField[T]

  // Type of skip list nodes (factored out for possible future modifications, i.e. indexed access)
  type SkipNode = Array[PrimRecord]

  // Max level of skip list pointers
  val MAX_LEVEL: Int = 32

  // Static state used during schema building
  final class State {
    // Totoal number of record fields
    private[EncoreSchemaFactory] var fieldCount: Int = 0

    // Totoal number of int record fields
    private[EncoreSchemaFactory] var intFieldCount: Int = 0

    // Totoal number of bool record fields
    private[EncoreSchemaFactory] var boolFieldCount: Int = 0

    // Totoal number of float record fields
    private[EncoreSchemaFactory] var floatFieldCount: Int = 0

    // Skip list heads
    private[EncoreSchemaFactory] var heads: Array[SkipNode] = null

    // Skip list max level of head nodes (1-based)
    private[EncoreSchemaFactory] var levels: Array[Int] = null
  }

  // PrimRecords aggregate field values by type into arrays
  final class PrimRecord(state: EncoreSchemaFactory.State) {
    // regular int field values
    private[EncoreSchemaFactory] val intMap: Array[Int] =
      if (state.intFieldCount == 0) null else new Array[Int]( state.intFieldCount )

    // regular bool field values
    private[EncoreSchemaFactory] val boolMap: BitSet =
      if (state.boolFieldCount == 0) null else new BitSet( state.boolFieldCount )

    // regular float field values
    private[EncoreSchemaFactory] val floatMap: Array[Float] =
      if (state.floatFieldCount == 0) null else new Array[Float]( state.floatFieldCount )

    // skip list forward pointers
    private[EncoreSchemaFactory] val ptrMap: Array[SkipNode] = new Array[SkipNode]( state.fieldCount )

    // skip list pred pointers
    // TODO: Refactor into ptrMap
    private[EncoreSchemaFactory] val predMap: Array[R] = new Array[R]( state.fieldCount )

    // used to avoid data structure corruption by invalid use (at the expense of increased record size)
    private[EncoreSchemaFactory] var inserted = new BitSet( state.fieldCount )
  }

  // PrimBuilder ties it all together; as fields get added their index and typeIndex fields are set
  final class PrimBuilder extends Builder {

    private var state = new State
    private var fields = Queue.newBuilder[F[_]]

    // PrimFields keep track of index and type index to store values into corresponding PrimRecord array slots
    class PrimField[T](aName: String, state: State)(implicit anIxs: IXS[T], aWdm: WDM[T], aMf: Manifest[T])
      extends Field[R, T](aName, anIxs, aWdm, aMf) {

      val index = state.fieldCount

      val typeIndex = mf match {
        case Manifest.Int => state.intFieldCount
        case Manifest.Boolean => state.boolFieldCount
        case Manifest.Float => state.floatFieldCount
        case _ => throw new IllegalArgumentException("Unsupported field type")
      }

      // Get value of field in record
      def get(record: R): T =
        mf match {
          case Manifest.Int => record.intMap(typeIndex).asInstanceOf[T]
          case Manifest.Boolean => record.boolMap(typeIndex).asInstanceOf[T]
          case Manifest.Float => record.floatMap(typeIndex).asInstanceOf[T]
        }

      // Set value of field in record
      def set(record: R, value: T) {
        if (record.inserted(index))
          throw new IllegalArgumentException("Attempt to modify already inserted record")
        else {
          mf match {
            case Manifest.Int => record.intMap(typeIndex) = value.asInstanceOf[Int]
            case Manifest.Boolean => record.boolMap(typeIndex) = value.asInstanceOf[Boolean]
            case Manifest.Float => record.floatMap(typeIndex) = value.asInstanceOf[Float]
          }
        }
      }

      // Create a generator that starting from query produces all other records stored in the
      // skip-list index of this field sorted according to w-weighted field distance
      //
      def iterator(w: Float, query: R): Iterator[(Float, R)] =
        new Iterator[(Float, R)] {
          var record = query
          var dist   = 0.0f

          findNext()

          def hasNext = record != null

          override def next() = {
            val result = (dist, record)
            findNext()
            result
          }

          private def findNext() {
            val left  = getPredPointer(record)

            if (left eq null) {
              record = null
              return
            }

            val left_distance = distance(w, query, left)
            val right = getForwardPointer(record, 0)

            if (right eq null) {
              record = left
              dist   = left_distance
              return
            }

            val right_distance = distance(w, query, right)

            if (left_distance < right_distance) {
              record = left
              dist   = left_distance
            } else {
              record = right
              dist   = right_distance
            }
          }
        }


      // Search for first record that has searchKey as value of this field in internal skip-list index and return it
      def search(searchKey: T): R = {
        var node: Array[R] = state.heads(index)

        var i = state.levels(index)
        var keyNode: R = null

        // empty list special case
        if (i == 0)
          return null

        var alreadyChecked: R = null
        do {
          i -= 1

          keyNode = node(i)
          while (keyNode != null && keyNode != alreadyChecked && wdm.lt(get(keyNode), searchKey)) {
            node = getForwardPointers(keyNode)
            keyNode = node(i)
          }
          alreadyChecked = keyNode
        } while (i > 0)

        if (keyNode eq null)
          null
        else {
          val key = get(keyNode)
          if (wdm.eq(key, searchKey)) keyNode else null
        }
      }

      // Insert record into internal skip-list index for field using the record's value of field
      private[EncoreSchemaFactory] def insert(record: R) {
        if (record.inserted(index))
          throw new IllegalArgumentException("Record already inserted")

        val head: Array[PrimRecord] = state.heads(index)
        var pred: R = null
        val headLevel = state.levels(index)

        var node: Array[R] = head
        var update: Array[SkipNode] = Array.ofDim(MAX_LEVEL)
        val searchKey: T = get(record)

        var i = headLevel

        // unless list is empty
        if (i > 0) {
          var keyNode: R = null
          var alreadyChecked: R = null
          do {
            i -= 1

            keyNode = node(i)
            while (keyNode != null && alreadyChecked != keyNode && wdm.lt(get(keyNode), searchKey)) {
              node    = getForwardPointers(keyNode)
              pred    = keyNode
              keyNode = node(i)
            }
            alreadyChecked = keyNode
            update(i) = node
          } while (i > 0)

          node = getForwardPointers(keyNode)
        }

        // we insert duplicates, so no update of existing node happens here
        // perhaps should get smarter about handling duplicates, though and keep them in a separate list
        // in order to be able to skip ahead to the next element

        val newLevel = randomLevel()
        if (newLevel > headLevel) {
          var j = newLevel
          do {
            j -= 1
            update(j) = head
          } while (j > 0)
          state.levels(index) = newLevel
        }

        val recordForwardPointers = ensureForwardPointerCapacity(record, newLevel)
        for (val i <- 0.until(newLevel)) {
          recordForwardPointers(i) = update(i)(i)
          update(i)(i) = record
        }
        record.inserted(index) = true
      }

      // 1-based !
      private def randomLevel(): Int = {
        var level = 0
        do {
          level += 1
        } while (level < MAX_LEVEL && math.random < 0.25d)
        level
      }

      // Get ith forward pointer of record for skip-list index of this field
      private[EncoreSchemaFactory] def getForwardPointer(record: R, i: Int): R = {
        val pointers = getForwardPointers(record)
        if (i < pointers.length)
          pointers(i)
        else {
          null
        }
      }

      // Set ith forward pointer of record for skip-list index of this field
      private[EncoreSchemaFactory] def setForwardPointer(record: R, i: Int, value: R) {
        ensureForwardPointerCapacity(record, i)(i) = value
      }

      // Ensure that the array of forward pointers in record used by the skip-list index of this field can
      // can store minLevels pointers by growing it if necessary
      //
      // (minLevels is capped automatically if it exceeds MAX_LEVEL)
      //
      private[EncoreSchemaFactory] def ensureForwardPointerCapacity(record: R, minLevels: Int): SkipNode = {
        val pointers = getForwardPointers(record)
        if (pointers eq null) {
          val newPointers = Array.ofDim[R](minLevels)
          setForwardPointers(record, newPointers)
          newPointers
        }
        else
          if (pointers.length < minLevels) {
            val newSize = math.min(math.max(minLevels, pointers.length * 2), MAX_LEVEL)
            val newPointers = Array.ofDim[R](newSize)
            Array.copy(pointers, 0, newPointers, 0, pointers.length)
            setForwardPointers(record, newPointers)
            newPointers
          }
          else
            pointers
      }

      // Get forward pointer array of record for the skip-list index of this field
      private[EncoreSchemaFactory] def getForwardPointers(record: R): SkipNode = record.ptrMap(index)

      // Set forward pointer array of record for the skip-list index of this field
      private[EncoreSchemaFactory] def setForwardPointers(record: R, value: SkipNode) {
        record.ptrMap(index) = value
      }

      // Get predecessor pointer of record for the skip-list index of this field
      private[EncoreSchemaFactory] def getPredPointer(record: R): R = record.predMap(index)

      // Set predecessor pointer of record for the skip-list index of this field
      private[EncoreSchemaFactory] def setPredPointer(record: R, value: R) {
        record.predMap(index) = value
      }
    }

    // Rewind builder
    def clear() {
      state = new State
      fields = Queue.newBuilder[F[_]]
    }

    // Add fields to currently build schema
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

    // Construct schema after all fields have been added
    //
    def result: Schema[R] = {
      state.heads = Array.ofDim[PrimRecord](state.fieldCount, MAX_LEVEL)
      state.levels = Array.ofDim[Int](state.fieldCount)

      val res = new PrimSchema(fields.result().toIndexedSeq, state)
      clear()
      res
    }
  }

  // Create builder for schema construction
  //
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
      for (field <- fields)
        field.insert(record)
    }

    // Retrieve nearest neighbors of record using the given weighting
    def search(weights: Weighting, record: PrimRecord, cont: Option[(Float, PrimRecord)] => Boolean) {

    }
  }

}

