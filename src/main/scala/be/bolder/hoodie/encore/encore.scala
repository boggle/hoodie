package be.bolder.hoodie.encore

import be.bolder.hoodie._
import scala.reflect.Manifest
import collection.mutable.{BitSet}
import collection.immutable.Queue
import java.lang.Thread.State
import sun.jvm.hotspot.debugger.posix.elf.ELFSectionHeader
import sun.tools.tree.WhileStatement
import com.sun.xml.internal.ws.developer.MemberSubmissionAddressing.Validation
import java.lang.IllegalArgumentException

// Pure in-memory implementation of the algorithm
object EncoreSchemaFactory extends SchemaFactory {
  override type R = PrimRecord
  override type B = PrimBuilder
  override type F[T] = B#PrimField[T]

  // Type of skip list nodes (factored out in case we need to go to indexed etc)
  type SkipNode = Array[PrimRecord]

  // Max level of skip list pointers
  val MAX_LEVEL: Int = 32

  // Static state used during schema building
  final class State {
    private[EncoreSchemaFactory] var fieldCount: Int = 0
    private[EncoreSchemaFactory] var intFieldCount: Int = 0
    private[EncoreSchemaFactory] var boolFieldCount: Int = 0
    private[EncoreSchemaFactory] var floatFieldCount: Int = 0

    // Skip list heads
    private[EncoreSchemaFactory] var heads: Array[SkipNode] = null
    // 1-based
    private[EncoreSchemaFactory] var levels: Array[Int] = null
  }

  // PrimRecords aggregate field values by type into arrays
  final class PrimRecord(state: EncoreSchemaFactory.State) {
    // regular int field values
    private[EncoreSchemaFactory] val intMap: Array[Int] =
      if (state.intFieldCount == 0) null else new Array[Int]( state.intFieldCount )

    private[EncoreSchemaFactory] val boolMap: BitSet =
      if (state.boolFieldCount == 0) null else new BitSet( state.boolFieldCount )

    private[EncoreSchemaFactory] val floatMap: Array[Float] =
      if (state.floatFieldCount == 0) null else new Array[Float]( state.floatFieldCount )

    // skip list pointers
    private[EncoreSchemaFactory] val ptrMap: Array[SkipNode] = new Array[SkipNode]( state.fieldCount )

    // used to avoid data structure corruption by invalid use at the expense of record size
    private[EncoreSchemaFactory] var inserted = new BitSet( state.fieldCount )
  }

  // PrimBuilder ties it all together; as fields get added their typeIndex is set
  final class PrimBuilder extends Builder {
    private var state = new State
    private var fields = Queue.newBuilder[F[_]]

    // PrimFields keep track of a type index to store values into corresponding PrimRecord array slots
    class PrimField[T](aName: String, state: State)(implicit anIxs: IXS[T], aWdm: WDM[T], aMf: Manifest[T])
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

      private[EncoreSchemaFactory] def insert(record: R) {
        if (record.inserted(index))
          throw new IllegalArgumentException("Record already inserted")

        val head: Array[PrimRecord] = state.heads(index)
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
              node = getForwardPointers(keyNode)
              keyNode = node(i)
            }
            alreadyChecked = keyNode
            update(i) = node
          } while (i > 0)

          node = getForwardPointers(keyNode)
        }

        // we insert duplicates, so no update of existing node happens here
        // should get smarter about handling duplicates, though

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

      private[EncoreSchemaFactory] def getForwardPointer(record: R, i: Int): R = {
        val pointers = getForwardPointers(record)
        if (i < pointers.length)
          pointers(i)
        else {
          null
        }
      }

      private[EncoreSchemaFactory] def setForwardPointer(record: R, i: Int, value: R) {
        ensureForwardPointerCapacity(record, i)(i) = value
      }

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

      private[EncoreSchemaFactory] def getForwardPointers(record: R): SkipNode = record.ptrMap(index)

      private[EncoreSchemaFactory] def setForwardPointers(record: R, value: SkipNode) {
        record.ptrMap(index) = value
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
      state.heads = Array.ofDim[PrimRecord](state.fieldCount, MAX_LEVEL)
      state.levels = Array.ofDim[Int](state.fieldCount)

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
      for (field <- fields)
        field.insert(record)
    }

    // Retrieve nearest neighbors of record using the given weighting
    def search(weights: Weighting, record: PrimRecord, cont: Option[(Float, PrimRecord)] => Boolean) {

    }
  }

}

