package be.bolder.hoodie

package chunkie {

  trait Chunk {
    type C >: this.type

    def manager: ChunkManager[C]

    @inline
    final def size = manager.size(this)

    @inline
    final def updateFrom(toPos: Int, src: C, srcPos: Int) { manager.write(src, srcPos, this, toPos) }

    @inline
    final def writeTo(fromPos: Int, dst: C, dstPos: Int) { manager.write(this, fromPos, dst, dstPos) }

    @inline
    final def swap(srcPos: Int, dst: C, dstPos: Int) { manager.swap(this, srcPos, dst, dstPos) }

    @inline
    final def updateFrom(toPos: Int, src: C, srcPos: Int, len: Int) { manager.write(src, srcPos, this, toPos, len) }

    @inline
    final def writeTo(fromPos: Int, dst: C, dstPos: Int, len: Int) { manager.write(this, fromPos, dst, dstPos, len) }

    @inline
    final def swap(srcPos: Int, dst: C, dstPos: Int, len: Int) { manager.swap(this, srcPos, dst, dstPos, len) }
  }

  trait ChunkManager[C] {
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

  trait MapChunk[R, V] {
    self: Chunk =>

    def getRecord(index: Int): R
    def setRecord(index: Int, newValue: R)

    def getValue(index: Int): V
    def setValue(index: Int, newValue: V)

    def apply(index: Int) = getValue(index)
  }

  trait SortableChunk[V] {
    self: Chunk with SortableChunk[V] =>

    override def manager: ChunkManager[C] with SortingChunkManager[C, V]

    def sort(off: Int, len: Int)(implicit ord: Ordering[V]) = manager.sort(self, off, len)
  }

  trait SortingChunkManager[C, V] {
    def sort(chunk: C, off: Int, len: Int)(implicit ord: Ordering[V])
  }

  object ParArrayChunks {
    final class PAManager[@specialized(Int, Long, Float, Double, Boolean) R,
                          @specialized(Int, Long, Float, Double, Boolean) V]
    (implicit mfR: Manifest[R], mfV: Manifest[V])
      extends ChunkManager[PAChunk[R, V]] with SortingChunkManager[PAChunk[R, V], V] {

      def ofSize(size: Int) = new PAChunk[R, V](PAManager.this, Array.ofDim[R](size), Array.ofDim[V](size))

      @inline
      def size(chunk: PAChunk[R, V]) = chunk.recs.length

      @inline
      def write(src: PAChunk[R, V], srcPos: Int, dst: PAChunk[R, V], dstPos: Int) {
        dst.recs(dstPos) = src.recs(srcPos)
        dst.vals(dstPos) = src.vals(srcPos)
      }

      @inline
      def write(src: PAChunk[R, V], srcPos: Int, dst: PAChunk[R, V], dstPos: Int, len: Int) {
        Array.copy(src.recs, srcPos, dst.recs, dstPos, len)
        Array.copy(src.vals, srcPos, dst.vals, dstPos, len)
      }

      @inline
      def swap(src: PAChunk[R, V], srcPos: Int, dst: PAChunk[R, V], dstPos: Int) {
        val oldSrcRec    = src.recs(srcPos)
        src.recs(srcPos) = dst.recs(dstPos)
        dst.recs(dstPos) = oldSrcRec

        val oldSrcVal    = src.vals(srcPos)
        src.vals(srcPos) = dst.vals(dstPos)
        dst.vals(dstPos) = oldSrcVal
      }

      @inline
      def swap(src: PAChunk[R, V], srcPos: Int, dst: PAChunk[R, V], dstPos: Int, len: Int) {
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

      def sort(x: PAChunk[R, V], off: Int, len: Int)(implicit ord: Ordering[V]) {
        // Shamelessly adapted from the scala library
        def med3(a: Int, b: Int, c: Int) = {
  	      val ab = ord.compare(x(a), x(b))
	        val bc = ord.compare(x(b), x(c))
	        val ac = ord.compare(x(a), x(c))
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
              while (j > off && (ord.compare(x(j-1), x(j)) > 0)) {
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
                var s = len / 8
                l = med3(l, l+s, l+2*s)
                m = med3(m-s, m, m+s)
                n = med3(n-2*s, n-s, n)
              }
              m = med3(l, m, n) // Mid-size, med of 3
            }
            val v = x(m)

            // Establish Invariant: v* (<v)* (>v)* v*
            var a = off
            var b = a
            var c = off + len - 1
            var d = c
            var done = false
            while (!done) {
              var bv = ord.compare(x(b), v)
              while (b <= c && bv <= 0) {
                if (bv == 0) {
                  swap(x, a, x, b)
                  a += 1
                }
                b += 1
                if (b <= c) bv = ord.compare(x(b), v)
              }
              var cv = ord.compare(x(c), v)
              while (c >= b && cv >= 0) {
                if (cv == 0) {
                  swap(x, c, x, d)
                  d -= 1
                }
                c -= 1
                if (c >= b) cv = ord.compare(x(c), v)
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

    final class PAChunk[@specialized(Int, Long, Float, Double, Boolean) R,
                        @specialized(Int, Long, Float, Double, Boolean) V]
    (override val manager: PAManager[R, V], val recs: Array[R], val vals: Array[V])
      extends Chunk with MapChunk[R, V] with SortableChunk[V] {
      type C = PAChunk[R, V]

      @inline
      def getRecord(index: Int) = recs(index)

      @inline
      def setRecord(index: Int, newRecord: R) { recs(index) = newRecord }

      @inline
      def getValue(index: Int) = vals(index)

      @inline
      def setValue(index: Int, newValue: V) { vals(index) = newValue }
    }
  }

  /*
  abstract class ArrayMapChunkManager[R, V] extends MapChunkManager[R, V, ArrayMapChunk[R, V]] {

    class AMChunk extends ArrayMapChunk[R, V] {
      val manager = ArrayMapChunkManager.this
      val chunk = AMChunk.this
    }
  }

  sealed class ArrayMapChunk[R, V] extends MapChunk[R, V] {
    type C = ArrayMapChunk[R, V]
  }
  */
}