package be.bolder.hoodie.linbin

import collection.mutable.BitSet
import scala.Array
import java.lang.{IllegalStateException, IllegalArgumentException}

final class StashOverflowException extends RuntimeException("Insert failure due to linbin stash overflow")

final class KeyNotFoundException extends NoSuchElementException("Provided key was not found")

trait LinBinOps[K, T] {

  def extractKey(item: T): K

  @throws(classOf[KeyNotFoundException])
  def apply(key: K): T

  def contains(key: K): Boolean

  def getDefault(key: K, defaultValue: T): T

  def getOption(key: K): Option[T]

  def +=(item: T): Boolean

  def -=(key: K): Boolean

  // def min: T

  // def max: T

  // def scanFrom(start: T): Iterator

  // def iterator: Iterator[T]

}

object LinBin {

  def stashSize(l: Int, m: Int) = ( ( 1 << (l+1) ) - 2 ) << m

  def verifyParameters(n: Int, l: Int, m: Int) {
    if (n < 1)
      throw new IllegalArgumentException("n < 1")
    if (n > 30)
       throw new IllegalArgumentException("n > 30")

    if (l < 0)
      throw new IllegalArgumentException("l < 0")
    if (l > 29)
       throw new IllegalArgumentException("l > 29")

    if (m < 0)
      throw new IllegalArgumentException("m < 0")
    if (m > 28)
      throw new IllegalArgumentException("m > 28")

    if ((l == 0) || (l + m > n))
      throw new IllegalArgumentException("l + m > n")
  }

  def fromItemSortedArray[@specialized(Short, Int, Long, Float, Double) T](n: Int, l: Int, m: Int, data: Array[T])
                           (implicit ord: Ordering[T],  mf: Manifest[T]): LinBin[T, T] = {
    verifyParameters(n, l, m)
    new ItemLinBin[T](n, l, m, data, Array.ofDim[T](stashSize(l, m)))
  }

  def fromKeySortedArray[@specialized(Short, Int, Long, Float, Double) K,
                         @specialized(Short, Int, Long, Float, Double) T]
                          (n: Int, l: Int, m: Int, data: Array[T])(extractor: (T => K))
                          (implicit ord: Ordering[K],  mf: Manifest[T]): LinBin[K, T] = {
    verifyParameters(n, l, m)
    new KeyLinBin[K, T](n, l, m, data, Array.ofDim[T](stashSize(l, m)))(extractor)
  }

  /*
   * This is the implementation class of LinBins
   *
   * See the companion object for docs
   *
   * @author Stefan Plantikow <stefan.plantikow@googlemail.com>
   *
   */
  sealed trait LinBin[@specialized(Short, Int, Long, Float, Double) K, @specialized(Short, Int, Long, Float, Double) T]
    extends LinBinOps[K, T] {

    // Number of elements in strip array = 2^n
    val n: Int

    // Number of stash levels
    val l: Int

     // Number of elements per stash block = 2^m
    val m: Int

    // Strip of presorted items
    protected val strip: Array[T]

    // Ordering used for sorting
    val ord: Ordering[K]

    // Manifest of item type
    protected val mf: Manifest[T]

    @inline
    def extractKey(item: T): K


    val maxStripSize   = 1 << n

    val stashBlockSize = 1 << m

    val stashBlockMask = ~ ( stashBlockSize - 1 )

    val numStashBlocks = ( 1 << (l+1) ) - 2

    val numChunkBits   = n - l

    val chunkSize      = 1 << numChunkBits


    private[this] def initStrip() {
      if (strip eq null)
        throw new NullPointerException
      if (strip.length > maxStripSize)
        throw new IllegalArgumentException("Provided data array too big for given parameters")
    }

    // Must go into separate function due to a bug  of @specialized
    // (compiler barfs when accessing strip array otherwise)
    initStrip()

    def stripSize = strip.length

    val stashSize = numStashBlocks << m

    protected val stash: Array[T]

    private val deletedFromStrip = BitSet( stripSize )

    private val valueInStash     = BitSet( stashSize )

    // End of initialization

    // Accessors
    //

    @throws(classOf[KeyNotFoundException])
    def apply(key: K): T = {
      // Find item in strip and stash
      val stripIndex = stripSearch(key, 0, strip.length)
      if ( (stripIndex >= 0) && (!deletedFromStrip(stripIndex)) )
        strip(stripIndex)
      else {
        val stashIndex = stashSearch(key, ~ stripIndex)
        if ( (stashIndex >=0) && valueInStash(stashIndex) )
          stash(stashIndex)
        else
          // If not found:
          throw new KeyNotFoundException
      }
    }

    def contains(key: K): Boolean = {
      // Find item in strip and stash
      val stripIndex = stripSearch(key, 0, strip.length)
      if ( (stripIndex >= 0) && (!deletedFromStrip(stripIndex)) )
        true
      else {
        val stashIndex = stashSearch(key, ~ stripIndex)
        if ( (stashIndex >=0) && valueInStash(stashIndex) )
          true
        else
          // If not found:
          false
      }
    }

    def getDefault(key: K, defaultValue: T): T = {
      // Find item in strip and stash
      val stripIndex = stripSearch(key, 0, strip.length)
      if ( (stripIndex >= 0) && (!deletedFromStrip(stripIndex)) )
        strip(stripIndex)
      else {
        val stashIndex = stashSearch(key, ~ stripIndex)
        if ( (stashIndex >=0) && valueInStash(stashIndex) )
          stash(stashIndex)
        else
          // If not found:
          defaultValue
      }
    }

    def getOption(key: K): Option[T] = {
      // Find item in strip and stash
      val stripIndex = stripSearch(key, 0, strip.length)
      if ( (stripIndex >= 0) && (!deletedFromStrip(stripIndex)) )
        Some(strip(stripIndex))
      else {
        val stashIndex = stashSearch(key, ~ stripIndex)
        if ( (stashIndex >=0) && valueInStash(stashIndex) )
          Some(stash(stashIndex))
        else
          // If not found:
          None
      }
    }


    def +=(item: T): Boolean = {
      val key = extractKey(item)

      // Search strip
      val stripIndex = stripSearch(key, 0, strip.length)

      if ( stripIndex >= 0 ) {
        // Found item in strip
        if (deletedFromStrip(stripIndex)) {
          // Un-delete it
          deletedFromStrip(stripIndex) = true
          true
        }
        else
          // Report (already deleted thus) non-existing item
          false
      } else {
        // Search stash
        val stashIndex = stashInsert(key, ~ stripIndex)

        if (stashIndex >= 0)
          // Report duplicate
          false
        else {
          // Insert into a stash block
          val insertPt   = ~ stashIndex
          val start     = blockStart(insertPt)
          val end       = blockEnd(start)
          val last      = lastInBlock(start, end)

          // It holds that start <= insertPt <= last < end

          if (insertPt <= last) {
            val cmp       = ord.compare(key, extractKey(stash(insertPt)))
            if (cmp < 0) { /* shift right and insert */
              Array.copy(stash, insertPt, stash, insertPt + 1, last - insertPt + 1)
              stash(insertPt) = item
            } else {  /* > 0 hop, shift right, and insert */
              Array.copy(stash, insertPt + 1, stash, insertPt + 2, last - insertPt)
              stash(insertPt + 1) = item
            }
            valueInStash(last + 1)= true
          } else {
            valueInStash(insertPt) = true
            stash(insertPt) = item
          }

          // Report success
          true
        }
      }
    }


    def -=(key: K): Boolean = {
      // Search strip
      val stripIndex = stripSearch(key, 0, strip.length)

      if ( stripIndex >= 0 ) {
        // Found item in strip
        if (deletedFromStrip(stripIndex)) {
          // Item not found
          false
        }
        else
          // Delete it
         deletedFromStrip(stripIndex) = true
        true
      } else {
        // Search stash
        val stashIndex = stashSearch(key, ~ stripIndex)
        if (stashIndex >= 0) {
          // remove from stash block
          val deletePt  = stashIndex
          val start     = blockStart(deletePt)
          val end       = blockEnd(start)
          val last      = lastInBlock(start, end)
          if (deletePt < last) {
            Array.copy(stash, deletePt + 1, stash, deletePt, last - deletePt)
          }
          valueInStash(last) = false

          // Report success
          true
        } else
          // Item not found
          false
      }
    }

    // Find key in strip starting from from to length using binary search
    //
    // If key is found, returns strip index
    //
    // Otherwise, returns - index - 1 of last field visited
    //
    // Does NOT consult deletedFromStrip
    //
    @inline
    private def stripSearch(key: K, from: Int, length: Int): Int = {
      val minIndex = from
      var maxIndex = from + length - 1
      var index    = minIndex

      while (true) {
        val midIndex = index + ((maxIndex-index) / 2)
        val midItem  = strip(midIndex)
        val midKey   = extractKey(midItem)
        val cmp      = ord.compare(key, midKey)
        if (cmp == 0)
          return midIndex
        else {
          if (cmp < 0)
            maxIndex = midIndex - 1
          else
            index    = midIndex + 1
        }

        if (index > maxIndex)
          // Negation works here since we don't use the upper bit anyways (n<=30)
          return ~ midIndex
      }

      throw new IllegalStateException("Unreachable")
    }

    // Search hierarchy of stash blocks starting from stash block block up to the top for key
    //
    // If key is found, returns stash index
    //
    // Otherwise, returns -1
    //
    // Does NOT consult emptyInStash except for finding block borders
    //
    @inline
    private def stashSearch(key: K, block: Int): Int  = {
      var level  = l
      var chunk  = chunkNr(block)
      var offset = 0

      while (true) {
        val minIndex = (offset | chunk) << m
        var maxIndex = lastInBlock(minIndex, blockEnd(minIndex))

        if (maxIndex < 0) {
          return -1
        }
        else {
          var index = minIndex
          var cont  = true
          while(cont) {
            val midIndex = index + ((maxIndex-index) / 2)
            val midItem  = stash(midIndex)
            val midKey   = extractKey(midItem)
            val cmp      = ord.compare(key, midKey)
            if (cmp == 0)
              return midIndex
            else {
              if (cmp < 0)
                maxIndex = midIndex - 1
              else
                index    = midIndex + 1
            }
            if (index > maxIndex) {
              if (level == 1)
                return -1
              else
                cont = false
            }
          }
        }
        chunk  >>= 1
        offset  += 1 << level
        level   -= 1
      }

      throw new IllegalStateException("Unreachable")
    }

    // Search hierarchy of stash blocks starting from stash block from up to the top for key
    //
    // If key is found, returns stash index
    //
    // Otherwise, returns where it would like to place the key in the form - stash index - 1
    //
    // If there is no free space left in the stash, throws StashOverflowException
    //
    // Does NOT consult emptyInStash except for finding block borders
    //
    @inline
    @throws(classOf[StashOverflowException])
    private def stashInsert(key: K, from: Int): Int  = {
      var level  =  l
      var chunk  =  chunkNr(from)
      var offset =  0
      var empty  = -1

      while (true) {
        // minIndex <= maxIndex <= blockEnd (except if block has no values at all in which case maxIndex == -1)
        val minIndex = ( offset | chunk ) << m
        val endIndex = blockEnd(minIndex)
        var maxIndex = lastInBlock(minIndex, endIndex)

        if (maxIndex < 0) {
         // We have reached a top level that is empty
          if (empty < 0)
            // Report minIndex as insertion point if none was found before
            return ~ minIndex
          else
            // Otherwise report previous insertion point
            return ~ empty
        }
        else {
          val free  = endIndex - maxIndex
          var index = minIndex
          var cont  = true
          while(cont) {
            val midIndex = index + ((maxIndex-index) / 2)
            val midItem  = stash(midIndex)
            val midKey   = extractKey(midItem)
            val cmp      = ord.compare(key, midKey)
            if (cmp == 0)
              return midIndex
            else {
              if (cmp < 0)
                maxIndex = midIndex - 1
              else
                index    = midIndex + 1
            }
            // break loop
            if (index > maxIndex) {
              // No insertion point found and free space in this level?
              if (empty < 0 && free > 0)
                // set insertion point
                empty = midIndex
              if (level == 1) {
                // last, topmost level
                if (empty < 0)
                  // No insertion point available
                  throw new StashOverflowException
                else
                  // Report insertion point
                  return ~ empty
              } else
                cont = false
            }
          }
        }
        chunk  >>= 1
        offset  += 1 << level
        level   -= 1
      }

      throw new IllegalStateException("Unreachable")
    }

    // Binary search on valueInStash bit field to determine the block length
    //
    // Returns last index in block that holds a value or -1 if no index in block holds a value
    //
    @inline
    private def lastInBlock(start: Int, end: Int): Int = {
      /* This is a variant of binary search that assumes the underlying data looks like
         either 0...0 or 1...1 or 1...1...0

         and searches for the last "1" (i.e. the first "10" substring)

         To do so, we define an order over neighbour bits and ensure the initial bit is not 0 before
         entering the binary search
       */
      if (! valueInStash(start))
        return -1

      val minIndex = start + 1
      var maxIndex = end

      var index    = minIndex
      while (index <= maxIndex) {
        val midIndex = index + ((maxIndex-index) / 2)

        val left     = valueInStash(midIndex - 1)
        val here     = valueInStash(midIndex)

        if (left) {
          if (here)
            maxIndex = midIndex - 1
          else
            return midIndex - 1
        } else
          index = midIndex + 1
      }
      end
    }

    /*
     * The object is broken after this call! For testing only
     */
    protected[hoodie] def simulateFullStash() {
      valueInStash.clear()
      for (i <- 0.until(stashSize))
        valueInStash(i) = true
    }

    @inline
    private def blockStart(index: Int): Int = index & stashBlockMask

    @inline
    private def blockEnd(startIndex: Int): Int = startIndex + stashBlockSize - 1

    @inline
    private def chunkNr(from: Int): Int = from >> numChunkBits /* n - l */


    /*

    private val levelOffsets = {
      val offsets = Array.ofDim[Int](l)

      for (level <- 1.until(l))
        offsets(level) = offsets(level-1) + (1 << (l-level+1))

      offsets
    }

    @inline
    private def blockAddr(from: Int, level: Int): Int = {
      // Compute chunk addr
      val chunk        = chunkNr(from)
      // Compute inverse level
      val inverseLevel = l - level
      // Compute number of stash block
      val blockOffset  = from >> inverseLevel
      val blockNr      = levelOffsets(inverseLevel) | blockOffset
      // Convert to stash index by shifting by block size
      blockNr << m
    }

    */

  }

  sealed private class ItemLinBin[@specialized(Short, Int, Long, Float, Double) T]
                                   (val n: Int, val l: Int, val m: Int,
                                    override protected val strip: Array[T],
                                    override protected val stash: Array[T])
                                   (implicit val ord: Ordering[T], val mf: Manifest[T])
    extends LinBin[T, T] {

    @inline
    def extractKey(item: T) = item
  }

  sealed private class KeyLinBin[@specialized(Short, Int, Long, Float, Double) K,
                                 @specialized(Short, Int, Long, Float, Double) T]
                                   (val n: Int, val l: Int, val m: Int,
                                    override protected val strip: Array[T],
                                    override protected val stash: Array[T])
                                   (val extractor: (T => K))
                                   (implicit val ord: Ordering[K], val mf: Manifest[T])
    extends LinBin[K, T] {

    @inline
    def extractKey(item: T) = extractor(item)
  }
}
