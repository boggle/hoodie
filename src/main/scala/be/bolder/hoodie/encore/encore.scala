package be.bolder.hoodie.encore

import be.bolder.hoodie._
import scala.reflect.Manifest
import collection.immutable.Queue
import util.control.Breaks._
import math.Ordering
import java.lang.{IllegalStateException, IllegalArgumentException}
import collection.mutable.{HashSet, BitSet, PriorityQueue}
import sun.jvm.hotspot.debugger.windbg.AddressDataSource

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
    // Total number of record fields
    private[EncoreSchemaFactory] var fieldCount: Int = 0

    // Total number of int record fields
    private[EncoreSchemaFactory] var intFieldCount: Int = 0

    // Total number of bool record fields
    private[EncoreSchemaFactory] var boolFieldCount: Int = 0

    // Total number of float record fields
    private[EncoreSchemaFactory] var floatFieldCount: Int = 0

    // Skip list heads
    private[EncoreSchemaFactory] var heads: Array[SkipNode] = null

    // Skip list max level of head nodes (1-based)
    private[EncoreSchemaFactory] var levels: Array[Int] = null

    // Marker array buffer
    // private[EncoreSchemaFactory] var abuf
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
    // TODO: Pull one level up
    final class PrimField[T](aName: String, state: State)(implicit anIxs: IXS[T], aWdm: WDM[T], aMf: Manifest[T])
      extends Field[R, T](aName, anIxs, aWdm, aMf) {

      override type F[T] = PrimField[T]

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

      // Iteration helper for PrimRecords
      protected[EncoreSchemaFactory] sealed abstract class RecIterator(w: Float, query: R)
        extends Iterator[(Float, R)] {

        var rec  = query
        var dist = 0.0f

        def nextRecord(aRec: R): R

        def findNext() {
          rec = nextRecord(rec);
          if (rec eq null)
            dist = Float.MaxValue
          else
            dist = distance(w, query, rec)
        }

        def hasNext = rec ne null

        def next() = if (hasNext) {
            val res = (dist, rec);
            findNext();
            res
          } else throw new IllegalStateException("Exhausted")
      }

      // Create an iterator that starting from query produces all other records stored in the
      // skip-list index that are smaller w.r.t to field value
      //
      // Iterator elements are pairs of (field distance to query, record)
      //
      // The iterator delivers the query point first
      //
      def predIterator(w: Float, query: R) = new RecIterator(w, query) {
        def nextRecord(aRec: R) = pred(aRec)
      }

      // Create an iterator that starting from query produces all other records stored in the
      // skip-list index that are larger w.r.t to field value
      //
      // Iterator elements are pairs of (field distance to query, record)
      //
      // The iterator delivers the query point first
      //
      def succIterator(w: Float, query: R) = new RecIterator(w, query) {
        def nextRecord(aRec: R) = succ(aRec)
      }

      // Create an iterator that starting from query produces all other records stored in the
      // skip-list index of this field sorted according to the w-weighted field distance
      //
      // Iterator elements are pairs of (field distance to query, record)
      //
      // The iterator delivers the query point first
      //
      def iterator(w: Float, query: R): Iterator[(Float, R)] = new Iterator[(Float, R)] {
          if (query eq null)
            throw new IllegalArgumentException("null query")

          val left  = predIterator(w, query)
          val right = succIterator(w, query)

          def hasNext = left.hasNext || right.hasNext

          def next() = (if (left.hasNext) {
                         if (right.hasNext) {
                           if (left.dist < right.dist) left else right
                         } else left
                       } else right).next()

          // Skip initial duplicate query point
          if (hasNext) next()
        }

      // If record has been inserted in this field's skip-list index, return it
      // Otherwise, searches for an arbitrary record with the same field value (i.e. calls search(get(record)))
      //
      def approx(record: R): R = if (record.inserted(index)) record else search(get(record))

      def head: Option[R] = if (state.levels(index) > 0) Some(state.heads(index)(0)) else None

      // Search for first record that has searchKey as value of this field in internal skip-list index and return it
      def search(searchKey: T): R = {
        val listHeader: Array[R] = state.heads(index)
        val listLevel            = state.levels(index)

        var x: R                 = null
        var x_forwards: Array[R] = listHeader
        var i                    = listLevel
        var alreadyChecked: R    = null

        do {
          // && keyNode != alreadyChecked
          while ((x_forwards(i) ne null) && (x_forwards(i) ne alreadyChecked) && wdm.lt(get(x_forwards(i)), searchKey))
          {
            x          = x_forwards(i)
            x_forwards = getForwardPointers(x)
          }
          alreadyChecked = x_forwards(i)

          i -= 1
        } while (i >= 0)

        x = x_forwards(0)

        if ((x ne null) && wdm.eq(get(x), searchKey)) x else null
      }

      // Insert record into internal skip-list index for field using the record's value of field
      private[EncoreSchemaFactory] def insert(record: R) {
        if (record.inserted(index))
          throw new IllegalArgumentException("Record already inserted")

        val listHeader: Array[R] = state.heads(index)
        val listLevel            = state.levels(index)

        val searchKey: T            = get(record)
        val update: Array[SkipNode] = Array.ofDim(MAX_LEVEL)

        var x: R                 = null
        var x_forwards: Array[R] = listHeader
        var i                    = listLevel
        var alreadyChecked: R    = null
        do {
          while ((x_forwards(i) ne null) && (x_forwards(i) ne alreadyChecked) && wdm.lt(get(x_forwards(i)), searchKey)) {
            x          = x_forwards(i)
            x_forwards = getForwardPointers(x)
          }
          alreadyChecked = x_forwards(i)
          update(i)      = x_forwards
          i -= 1

        } while (i >= 0)

        // we insert duplicates, so no update of existing node happens here
        // perhaps should get smarter about handling duplicates, though and keep them in a separate list
        // in order to be able to skip ahead to the next record with a larger field value

        {
          // Adjust pred pointers
          setPredPointer(record, x)
          val x0 = x_forwards(0)
          if (x0 ne null) setPredPointer(x0, record)
        }

        val newLevel = randomLevel()
        if (newLevel > listLevel) {
          var j = newLevel - 1
          while (j > listLevel) {
            update(j) = listHeader
            j -= 1
          }
          state.levels(index) = newLevel
        }

        x          = record
        x_forwards = ensureForwardPointerCapacity(record, newLevel)

        // var ptr: R = update(0)(0)
        // if (ptr ne null) {
        //   setPredPointer(x, ptr)
          // setPredPointer(ptr, x)
        // }

        for (i <- 0.until(newLevel)) {
          x_forwards(i) = update(i)(i)
          update(i)(i)  = x
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

      // Predecessor in skip-list for this field
      def pred(record: R): R = getPredPointer(record)

      // Successor in skip-list for this field
      def succ(record: R): R = getForwardPointer(record, 0)

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
            val newPointers = Array.ofDim[R](math.min(minLevels, MAX_LEVEL))
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
    def result: PrimSchema = {
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

    def mkRecord: R = new PrimRecord(state)

    def mkFromString(lineStr: String): R = {
      val record  = mkRecord
      setFromString(record, lineStr)
      record
    }

    // Insert record into index structure
    def insert(record: R) {
      for (field <- fields)
        field.insert(record)
    }

    // Retrieve nearest neighbors of record using the given weighting
    def search(weights: Weighting, query: R)(cont: Option[(Float, R)] => Boolean) {
      if (weights.length != fields.length)
        throw new IllegalArgumentException("Invalid weights (wrong number of elements) provided")

      val maxDists = Array.ofDim[Float](fields.length)

      val ordering = Ordering.Float.reverse.on { (outer: (Float, R)) => outer._1}
      val cands    = new PriorityQueue[(Float, R)]()(ordering)
      val set      = new HashSet[R]()
      var cut      = Float.NegativeInfinity
      var iters    = fields.zipWithIndex.map { case (field, i) =>
        (field, field.iterator(weights(i), field.approx(query)))
      }.filter( _._2.hasNext )

      while(iters.nonEmpty) {
        // Compute all next values, update maxDists, and add to cands
        for (entry <- iters) {
          val field   = entry._1
          val i       = field.index
          val iter    = entry._2

          var adds    = false
          var rec: R  = null
          do {
            val elem = iter.next()
                rec  = elem._2
                adds = !set.contains(rec)

            if (adds) {
              val dimDist = elem._1
              // if (dimDist < maxDists(i))
              //   throw new IllegalStateException("Monotonicity violation")
              val dimSq = dimDist * dimDist
              val maxSq = maxDists(i)
              if (dimSq > maxSq) {
                // Update cut value on the fly
                cut        += (maxSq - dimSq)
                maxDists(i) = dimSq
              }
              val recDist = distanceSquare(weights, query, rec)
              set   += rec
              cands += ((recDist, rec))
            }
          } while (!adds && iter.hasNext)
        }

        breakable { while (cands.nonEmpty) {
          val elem = cands.dequeue()
          if (elem._1 <= cut) {
            if (!cont(Some( (math.sqrt(elem._1.toDouble).toFloat, elem._2) )))
              return
          } else {
            cands += elem
            break
          }
        } }

        // Drop empty iterators for next round
        iters = iters.filter( _._2.hasNext )
      }

      // Deliver remaining results
      while (cands.nonEmpty) {
        val elem = cands.dequeue()
        if (!cont(Some( (math.sqrt(elem._1.toDouble).toFloat, elem._2) )))
          return
      }

      // Signal end of result stream to greedy consumer
      cont(None)
    }
  }
}

