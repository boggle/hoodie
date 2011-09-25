package be.bolder.hoodie

package chunkie {

  /**
   * ChunkOps instances describe how to instantiate and write values between opaque Int-indexed chunk representations of
   * type C
   */
  trait ChunkOps[C] {
    // Reifies C for type projections
    final type Repr = C

    /**
     * Expected to be < (1 << 31) !
     */
    def ofSize(size: Int): C

    def empty: C = ofSize(0)

    def copy(src: C): C = copy(src, 0, size(src))

    def copy(src: C, startPos: Int, len: Int): C = {
      val result = ofSize(len)
      write(src, startPos, result, 0, len)
      result
    }

    def size(chunk: C): Int

    def write(src: C, srcPos: Int, dst: C, dstPos: Int)
    def write(src: C, srcPos: Int, dst: C, dstPos: Int, len: Int)

    def swap(src: C, srcPos: Int, dst: C, dstPos: Int)
    def swap(src: C, srcPos: Int, dst: C, dstPos: Int, len: Int)

  }

  /**
   * A ChunkWrap contains a chunk representation and has an interface for redirecting operations on its internal state
   * to an appropriate ChunkOps instance
   */
  trait ChunkWrap {
    type C >: this.type

    def ops: ChunkOps[C]

    @inline
    final def size = ops.size(this)

    @inline
    final def updateFrom(toPos: Int, src: C, srcPos: Int) { ops.write(src, srcPos, this, toPos) }

    @inline
    final def writeTo(fromPos: Int, dst: C, dstPos: Int) { ops.write(this, fromPos, dst, dstPos) }

    @inline
    final def swap(srcPos: Int, dst: C, dstPos: Int) { ops.swap(this, srcPos, dst, dstPos) }

    @inline
    final def updateFrom(toPos: Int, src: C, srcPos: Int, len: Int) { ops.write(src, srcPos, this, toPos, len) }

    @inline
    final def writeTo(fromPos: Int, dst: C, dstPos: Int, len: Int) { ops.write(this, fromPos, dst, dstPos, len) }

    @inline
    final def swap(srcPos: Int, dst: C, dstPos: Int, len: Int) { ops.swap(this, srcPos, dst, dstPos, len) }
  }

  /**
   * KeyedChunkOps associate a key of type V to every entry in a chunk representation C
   */
  trait KeyedChunkOps[C, V] {
    self: ChunkOps[C] =>

    def key(chunk: C, index: Int): V
  }


  /**
   * Wrapper trait for chunk representations with KeyedChunkOps support
   */
  trait KeyedChunkWrap[V] {
    self: ChunkWrap with KeyedChunkWrap[V] =>

    override def ops: ChunkOps[C] with KeyedChunkOps[C, V]

    @inline
    final def key(index: Int): V = ops.key(this, index)
  }

  /**
   * SearchingChunkOps support a search operation on chunk representations of type C
   */
  trait SearchChunkOps[C, V] {
    self: ChunkOps[C] with KeyedChunkOps[C, V] with SearchChunkOps[C, V] =>

    /**
     * Search for value in chunk in the range from, to (inclusive) assuming that range has been sorted
     * according to ord in ascending order
     *
     * @return index of first occurrence of value if one is found or negated (~) index of last item checked
     */
    def search(chunk: C, from: Int, to: Int, value: V)(implicit ord: Ordering[V]): Int
  }


  /**
   * SearchingChunkOps implementation based in linear search
   */
  trait LinearSearchChunkOps[C, V] extends SearchChunkOps[C, V] {
    self: ChunkOps[C] with KeyedChunkOps[C, V] with LinearSearchChunkOps[C, V] =>

    @inline
    final protected def linearSearch(chunk: C, from: Int, to: Int, value: V)(implicit ord: Ordering[V]): Int = {
      var _i = from
      while (_i <= to) {
          val cmp = ord.compare(value, key(chunk, _i))
          if (cmp == 0)
            return _i
          else {
            if (cmp < 0)
              _i += 1
            else
              return ~ _i
          }
      }
      ~ to
    }

    def search(chunk: C, from: Int, to: Int, value: V)(implicit ord: Ordering[V]): Int = {
      if (from > to)
        throw new IllegalArgumentException("from > to")
      else
        linearSearch(chunk, from, to, value)
    }
  }

  object BinarySearchChunkOps {
      // Practical constant, in 2011 typical x86 boxes will perform linear scan of 50-100 elements faster than
      // binary search
    val LINEAR_STRIDE_SIZE = 64
  }

  /**
   * SearchingChunkOps implementation based on binary search that falls back on linear search for small
   * ranges (<64 elements)
   */
  trait BinarySearchChunkOps[C, V] extends LinearSearchChunkOps[C, V] {
    self: ChunkOps[C] with KeyedChunkOps[C, V] with BinarySearchChunkOps[C, V] =>

    override def search(chunk: C, from: Int, to: Int, value: V)(implicit ord: Ordering[V]): Int = {
      if (from > to)
        throw new IllegalArgumentException("from > to")
      else {
        val numElts = to - from
        if (numElts < BinarySearchChunkOps.LINEAR_STRIDE_SIZE)
          linearSearch(chunk, from, to, value)
        else {
          var maxIndex = to
          var minIndex = from
          if (minIndex < maxIndex) {
            while (true) {
              val midIndex = minIndex + (maxIndex - minIndex)/2
              val cmp      = ord.compare(value, key(chunk, midIndex))
              if (cmp == 0)
                return midIndex
              else {
                if (cmp < 0)
                  maxIndex = midIndex - 1
                else
                  minIndex = midIndex + 1
              }
              if (! (minIndex < maxIndex))
                return ~ midIndex
            }
          }
          ~ minIndex
        }
      }
    }
  }

  /**
   * Wrapper trait for chunk representations with SearchChunkOps support
   */
  trait SearchChunkWrap[V] {
    self: ChunkWrap with KeyedChunkWrap[V] with SearchChunkWrap[V] =>

    override def ops: ChunkOps[C] with SearchChunkOps[C, V]

    @inline
    final def search(from: Int, to: Int, value: V)(implicit ord: Ordering[V]): Int = ops.search(this, from, to, value)
  }

  /**
   * SortChunkOps instances provide a sort operation on V-keyed chunk representations of type C
   */
  trait SortChunkOps[C, V] {
    self: ChunkOps[C] with KeyedChunkOps[C, V] with SortChunkOps[C, V] =>

    /**
     * Sort C in the inclusive range (off, off+len-1) according to ord
     */
    def sort(chunk: C, off: Int, len: Int)(implicit ord: Ordering[V])
  }

  /**
   * SortChunkOps implementation based on quick sort
   */
  trait QuickSortChunkOps[C, V] extends SortChunkOps[C, V] {
    self: ChunkOps[C] with KeyedChunkOps[C, V] with QuickSortChunkOps[C, V] =>


    final def sort(x: C, off: Int, len: Int)(implicit ord: Ordering[V]) {
      // Shamelessly adapted from the scala library
      def med3(a: Int, b: Int, c: Int) = {
        val ab = ord.compare(key(x, a), key(x, b))
        val bc = ord.compare(key(x, b), key(x, c))
        val ac = ord.compare(key(x, a), key(x, c))
        if (ab < 0) {
          if (bc < 0) b else if (ac < 0) c else a
        } else {
         if (bc > 0) b else if (ac > 0) c else a
        }
      }

      def sort2(off: Int, len: Int) {
        // Insertion sort on smallest arrays
        if (len < 7) {
          var i = off
          while (i < len + off) {
            var j = i
            while (j > off && (ord.compare(key(x, j-1), key(x, j)) > 0)) {
              swap(x, j, x, j-1)
              j -= 1
            }
            i += 1
          }
        } else {
          // Choose a partition element, v
          var m = off + (len >> 1)        // Small arrays, middle element
          if (len > 7) {
            var l = off
            var n = off + len - 1
            if (len > 40) {        // Big arrays, pseudomedian of 9
              val s = len / 8
              l = med3(l, l+s, l+2*s)
              m = med3(m-s, m, m+s)
              n = med3(n-2*s, n-s, n)
            }
            m = med3(l, m, n) // Mid-size, med of 3
          }
          val v = key(x, m)

          // Establish Invariant: v* (<v)* (>v)* v*
          var a = off
          var b = a
          var c = off + len - 1
          var d = c
          var done = false
          while (!done) {
            var bv = ord.compare(key(x, b), v)
            while (b <= c && bv <= 0) {
              if (bv == 0) {
                swap(x, a, x, b)
                a += 1
              }
              b += 1
              if (b <= c) bv = ord.compare(key(x, b), v)
            }
            var cv = ord.compare(key(x, c), v)
            while (c >= b && cv >= 0) {
              if (cv == 0) {
                swap(x, c, x, d)
                d -= 1
              }
              c -= 1
              if (c >= b) cv = ord.compare(key(x, c), v)
            }
            if (b > c) {
              done = true
            } else {
              swap(x, b, x, c)
              c -= 1
              b += 1
            }
          }

          // Swap partition elements back to middle
          val n = off + len
          var s = math.min(a-off, b-a)
          swap(x, off, x, b-s, s)
          s = math.min(d-c, n-d-1)
          swap(x, b, x, n-s, s)

          // Recursively sort non-partition-elements
          s = b - a
          if (s > 1)
            sort2(off, s)
          s = d - c
          if (s > 1)
            sort2(n-s, s)
        }
      }
      sort2(off, len)
    }
  }

  /**
   * Wrapper trait for chunk representations with SortChunkOps support
   */
  trait SortChunkWrap[V] {
    self: ChunkWrap with KeyedChunkWrap[V] with SortChunkWrap[V] =>

    override def ops: ChunkOps[C] with KeyedChunkOps[C, V] with SortChunkOps[C, V]

    @inline
    final def sort(off: Int, len: Int)(implicit ord: Ordering[V]) = ops.sort(self, off, len)
  }


  /**
   * MapChunksOps instances provide key-value data per Int-indexed entry in a chunk representation of type C
   */
  trait MapChunkOps[C, R, V] {
    self: ChunkOps[C] with KeyedChunkOps[C, V] with MapChunkOps[C, R, V] =>

    def getRecord(chunk: C, index: Int): R
    def setRecord(chunk: C, index: Int, newValue: R)

    def getValue(chunk: C, index: Int): V
    def setValue(chunk: C, index: Int, newValue: V)

    @inline
    final def key(chunk: C, index: Int) = getValue(chunk, index)
  }


  /**
   * Wrapper trait for chunk representations with MapChunkOps support
   */
  trait MapChunkWrap[R, V] {
    self: ChunkWrap with KeyedChunkWrap[V] with MapChunkWrap[R, V] =>

    override def ops: ChunkOps[C] with KeyedChunkOps[C, V] with MapChunkOps[C, R, V]

    @inline
    final def getRecord(index: Int): R = ops.getRecord(this, index)

    @inline
    final def setRecord(index: Int, newValue: R) { ops.setRecord(this, index, newValue) }

    @inline
    final def getValue(index: Int): V = ops.getValue(this, index)

    @inline
    final def setValue(index: Int, newValue: V) { ops.setValue(this, index, newValue) }
  }


  /**
   * Bundles several useful ChunkOps over chunk representations of type C together
   */
  trait DefaultChunkOps[C, R, V] extends ChunkOps[C]
    with KeyedChunkOps[C, V]
    with MapChunkOps[C, R, V]
    with SearchChunkOps[C, V]
    with SortChunkOps[C, V] {

    self: DefaultChunkOps[C, R, V] =>
  }

  /**
   * Wrapper trait for chunk representations with DefaultChunkOps support
   */  
  trait DefaultChunkWrap[R, V] extends ChunkWrap
    with KeyedChunkWrap[V]
    with MapChunkWrap[R, V]
    with SearchChunkWrap[V]
    with SortChunkWrap[V] {

    self: DefaultChunkWrap[R, V] =>

    override def ops: DefaultChunkOps[C, R, V]
  }

  /**
   * Implementation of DefaultChunkOps that just uses a single array to store records internally and
   * extracts/updates values using explicitly provided getter/setter lambdas
   *
   * Advantage: Requires minimal space
   * Disadvantage: Accessing record values may lead to more cache misses
   */
  class SingleArrayMapChunks[@specialized(Int, Long, Float, Double, Boolean) R,
                             @specialized(Int, Long, Float, Double, Boolean) V]
    (val valueGetter: R => V, val valueSetter: (R, V) => Unit)
    (implicit mfR: Manifest[R], mfV: Manifest[V]) {

    final class SAMChunkOps
      extends DefaultChunkOps[SAMChunkWrap, R, V]
      with BinarySearchChunkOps[SAMChunkWrap, V]
      with QuickSortChunkOps[SAMChunkWrap, V] {

    def ofSize(size: Int) = new SAMChunkWrap(SAMChunkOps.this, Array.ofDim[R](size), Array.ofDim[V](size))

    @inline
    def size(chunk: SAMChunkWrap) = chunk.recs.length

    @inline
    def write(src: SAMChunkWrap, srcPos: Int, dst: SAMChunkWrap, dstPos: Int) {
      dst.recs(dstPos) = src.recs(srcPos)
    }

    @inline
    def write(src: SAMChunkWrap, srcPos: Int, dst: SAMChunkWrap, dstPos: Int, len: Int) {
      Array.copy(src.recs, srcPos, dst.recs, dstPos, len)
    }

    @inline
    def swap(src: SAMChunkWrap, srcPos: Int, dst: SAMChunkWrap, dstPos: Int) {
      val oldSrcRec    = src.recs(srcPos)
      src.recs(srcPos) = dst.recs(dstPos)
      dst.recs(dstPos) = oldSrcRec
    }

    @inline
    def swap(src: SAMChunkWrap, srcPos: Int, dst: SAMChunkWrap, dstPos: Int, len: Int) {
      var _s = srcPos
      var _d = dstPos
      var _i = 0
      while (_i < len) {
        swap(src, _s, dst, _d)
        _s += 1
        _d += 1
        _i += 1
      }
    }


    @inline
    def getRecord(chunk: SAMChunkWrap, index: Int) = chunk.recs(index)

    @inline
    def setRecord(chunk: SAMChunkWrap, index: Int, newRecord: R) { chunk.recs(index) = newRecord }

    @inline
    def getValue(chunk: SAMChunkWrap, index: Int) = valueGetter(chunk.recs(index))

    @inline
    def setValue(chunk: SAMChunkWrap, index: Int, newValue: V) {
      valueSetter(chunk.recs(index), newValue)
    }
  }

  final class SAMChunkWrap(override val ops: SAMChunkOps, val recs: Array[R], val vals: Array[V])
    extends DefaultChunkWrap[R, V] {

    type C = SAMChunkWrap
  }
 }

  /***
   * Implementation of DefaultChunkOps that uses two arrays indexed in parallel to store records and values
   *
   * Advantage: Better cache behaviour (Main reason to use this, really)
   * Disadvantage: Requires extra space for values array; Cost of some operations doubles; No syncing of
   * value updates back to records (your job)
   */
  object ParArrayMapChunks {
    final class PAMChunkOps[@specialized(Int, Long, Float, Double, Boolean) R,
                            @specialized(Int, Long, Float, Double, Boolean) V]
    (implicit mfR: Manifest[R], mfV: Manifest[V])
      extends DefaultChunkOps[PAMChunkWrap[R, V], R, V]
      with BinarySearchChunkOps[PAMChunkWrap[R, V], V]
      with QuickSortChunkOps[PAMChunkWrap[R, V], V] {

      def ofSize(size: Int) = new PAMChunkWrap[R, V](PAMChunkOps.this, Array.ofDim[R](size), Array.ofDim[V](size))

      @inline
      def size(chunk: PAMChunkWrap[R, V]) = chunk.recs.length

      @inline
      def write(src: PAMChunkWrap[R, V], srcPos: Int, dst: PAMChunkWrap[R, V], dstPos: Int) {
        dst.recs(dstPos) = src.recs(srcPos)
        dst.vals(dstPos) = src.vals(srcPos)
      }

      @inline
      def write(src: PAMChunkWrap[R, V], srcPos: Int, dst: PAMChunkWrap[R, V], dstPos: Int, len: Int) {
        Array.copy(src.recs, srcPos, dst.recs, dstPos, len)
        Array.copy(src.vals, srcPos, dst.vals, dstPos, len)
      }

      @inline
      def swap(src: PAMChunkWrap[R, V], srcPos: Int, dst: PAMChunkWrap[R, V], dstPos: Int) {
        val oldSrcRec    = src.recs(srcPos)
        src.recs(srcPos) = dst.recs(dstPos)
        dst.recs(dstPos) = oldSrcRec

        val oldSrcVal    = src.vals(srcPos)
        src.vals(srcPos) = dst.vals(dstPos)
        dst.vals(dstPos) = oldSrcVal
      }

      @inline
      def swap(src: PAMChunkWrap[R, V], srcPos: Int, dst: PAMChunkWrap[R, V], dstPos: Int, len: Int) {
        var _s = srcPos
        var _d = dstPos
        var _i = 0
        while (_i < len) {
          swap(src, _s, dst, _d)
          _s += 1
          _d += 1
          _i += 1
        }
      }


      @inline
      def getRecord(chunk: PAMChunkWrap[R, V], index: Int) = chunk.recs(index)

      @inline
      def setRecord(chunk: PAMChunkWrap[R, V], index: Int, newRecord: R) { chunk.recs(index) = newRecord }

      @inline
      def getValue(chunk: PAMChunkWrap[R, V], index: Int) = chunk.vals(index)

      @inline
      def setValue(chunk: PAMChunkWrap[R, V], index: Int, newValue: V) { chunk.vals(index) = newValue }
    }

    final class PAMChunkWrap[@specialized(Int, Long, Float, Double, Boolean) R,
                            @specialized(Int, Long, Float, Double, Boolean) V]
    (override val ops: PAMChunkOps[R, V], val recs: Array[R], val vals: Array[V])
      extends DefaultChunkWrap[R, V] {

      type C = PAMChunkWrap[R, V]
    }
  }
}