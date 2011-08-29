package be.bolder.hoodie.encore.linbin

import math.BigInt
import util.Sorting
import java.io.DataInput
import java.lang.{IllegalStateException, IllegalArgumentException}

sealed class StashOverflowException extends RuntimeException("Insert failure due to linbin stash overflow")

object StashOverflowException extends StashOverflowException


/*
 * This is a factory class for linbins.
 *
 * A linbin is a mix between a sorted array and an binary tree. Both data structures are interspersed with each other.
 *
 * The array is for fast key search; while the tree buffers inserts in a cache conscious way to avoid having to
 * sort the array too often
 *
 * Intended use is as a *building*block* for dictionary structures that need very fast, very frequent lookup of sorted
 * keys but need to handle a small to medium number of inserts, and deletes, too
 *
 * The idea is to intersperse the tree (called the stash) with the array (called the core) in such a way as to get
 *
 * (1) good cache locality
 *
 * (2) at most a small number of linear compares when searching the stash after not having found an item in the core
 *     (Parameters are chosen as to make stash search not more expensive than a second logn lookup assuming
 *      2011 hardware, i.e. roughly 50-100 linear scans corresponding to a log lookup)
 *
 * (3) assuming inserts are not totally skewed, a much higher number of inserts before the linbin needs to be
 *     resorted/split for further inserts.
 *
 * (4) Additionally, linbins can turn into classic sorted constant array on request / when full
 *
 * The interspersing of core (array) and stash (tree) is best explained with an example. In the following,
 * "X" is a stash section, "O" is a data section (number of bits are parametrized during LinBin factory construction)
 *
 * Assume numStashLayers = 3. You get the following layout
 *
 * X X X O X O X X O X O X X X O X O X X O X O
 *
 * Assume you searched for an item k, and your closest match for k is "?":
 *
 * X X X O X O X X O X ? X X X O X O X X O X O
 *
 *  Linear stash search will also check numStashLayers stash entries marked with "!":
 *
 * ! X X O X O ! X O ! ? X X X O X O X X O X O
 *
 * These contain about 50 items using the default parameters of LinBinFactory.DefaultXYZ factory instances
 *
 * If your closest match was "-":
 *
 * X X X O X O X X - X ? X X X O X O X X O X O
 *
 * The stash would be searched at
 *
 * ! X X O X O ! ! - X ? X X X O X O X X O X O
 *
 * i.e. different stash searches may use the same elements. Therefore the tree like stash acts a bit
 * like a hash table collision strategy
 *
 * On insert, first the relevant core section is found (using binary search over the core elements without looking at
 * the stash). Then from closest, to farthest, the stash elements are checked for a free position for the inserted item
 * using linear search.  If one is found, the item is put there and the stash element is resorted
 *
 * When an item cannot be inserted, the whole underlying array is sorted and the linbin becomes a regular sorted
 * array and needs to be split/added to new linbins in order to be available for insertion again.
 *
 * For large stashes, each stash section may be kept sorted (IDEA; NOT IMPLEMENTED)
 *
 * Additionally, as long is there is still room, items may be added for plain linear scanning at the end of the
 * underlying array after the stash has been exhausted (IDEA; NOT IMPLEMENTED)
 *
 * @author Stefan Plantikow <stefan.plantikow@googlemail.com>
 *
 */
sealed class LinBinFactory(val elemAddrBits: Int,       /* log_2(num sorted elements) */
                           val stashLayout: (Int, Int)) /* (Num stash layers, log_2 (num elements per stash block)) */
{
  final val numStashLevels = stashLayout._1
  final val stashNodeBits  = stashLayout._2
  final val stashNodeSize  = 1 << stashNodeBits

  {
    // Preconditions that verify constructor parameters

    if (elemAddrBits < 4)
        throw new IllegalArgumentException("elemAddrBits < 4")

    if (numStashLevels < 1)
        throw new IllegalArgumentException("numStashLayers < 1")

    if (stashNodeSize <= 0)
        throw new IllegalArgumentException("stashNodeSize negative or zero")

    if (numStashLevels > elemAddrBits)
        throw new IllegalArgumentException("stashAddrBits > elemAddrBits")


  }

  // Number of elements to be stored directly in the sorted portion of the array
  val numCoreElements   = 1 << elemAddrBits

  // Maximum number of elements to be stored in the stash
  val numStashElements  = {
    val value = BigInt(2).pow(numStashLevels+1).-(2).*(stashNodeSize)
    if (value > Int.MaxValue)
      throw new IllegalArgumentException("Stash is too large")
    value.toInt
  }

  // Size of the underlying array of linbins created by this factory
  private val numArrayElements  = {
    val value = BigInt(2).pow(elemAddrBits) + numStashElements

    if (value > Int.MaxValue)
      throw new IllegalArgumentException("Parameters to large to yield addressable array")

    numCoreElements + numStashElements
  }

  // Size of sorted chunks between stash sections
  private val coreLeafSize = numCoreElements >> numStashLevels

  private val coreMidPoint = numCoreElements >> 1

  // The following precomputed arrays are used in linbin instances to address the underlying array
  // without having to recompute the interspersing again and again

  // Maps from index of element in "virtual" array of all core elements to position in underlying array
  private val eltsIndex    = Array.fill[Int](numCoreElements)(-1)

  // Maps from position in underlying array to position of parent stash element in underlying array
  private val stashParents = Array.fill[Int](numArrayElements)(-1)

  {
    // Computes eltsIndex and stashParents

    // Constants for computing core and stash interleaving
    //
    // bitSum(i) is the distance for moving over half the array of stash and core elements to the opposing stash
    // element at level i
    //
    // which is      ( 2^(i+1)-1 ) * stashNodeSize + ( 2^i * coreLeafSize )
    // optionally  + ( numStashLayers * stashNodeSize ) [ <-- excluded here and added implicitly by markLevel below ]
    //
    val bitSums = ( for(i <- 0.until(numStashLevels))
                          yield (  (((1 << (i + 1)) - 1) * stashNodeSize)
                                 + ((1 << i) * coreLeafSize)
                                ) ).toArray

    def markLevel(parent: Int, leftSum: Int, level: Int) {
      if (level < 0)
          return

      for (i <- leftSum.until(leftSum+stashNodeSize))
        stashParents(i) = parent
      markLevel(leftSum, leftSum+stashNodeSize, level-1)

      val rightSum = leftSum + bitSums(level)
      for (i <- rightSum.until(rightSum+stashNodeSize))
        stashParents(i) = parent
      markLevel(rightSum, rightSum+stashNodeSize, level-1)
    }

    // Construct parent tree in stashParents array
    markLevel(0, stashNodeSize, numStashLevels-1)

    // Construct linear order from stashParents array in eltsIndex array
    var count = 0
    for (i <- 0.until(numArrayElements)) {
      val parent = stashParents(i)
      if (parent < 0) {
        eltsIndex(count) = i
        count += 1
      }
    }

    // Fix stashParents array such that core leaves point to their leftmost stash node as their parent
    var parent = 0
    var prev   = -1
    for (j <- 0.until(stashParents.length)) {
      val cur = stashParents(j)
      if (cur >= 0) {
        if (cur != prev)
          parent = j
      }
      else
        stashParents(j) = parent
      prev = cur
    }

    // Set parent of root element to -1 (helps with looping, resp. loop abortion)
    stashParents(0) = -1
  }

  // Create an empty linbin
  def empty[T](emptyValue: T)(implicit mf: Manifest[T], ord: Ordering[T]) =
    new LinBin[T]( emptyValue, Array.fill[T]( numArrayElements )( emptyValue ), 0, 0 )

  // Creates a new linbin from the seqLength data items in seq
  //
  // Expects seq to be sorted in accordance with ord. This is not checked and your responsibility
  //
  def fromSortedSeq[T](emptyValue: T)(seq: Seq[T], seqLength: Int)
                      (implicit mf: Manifest[T], ord: Ordering[T]): LinBin[T] =
  {
    // We check this as it is an easy mistake, all other misalignment will turn into IndexOutOfBounds
    if (seqLength > numCoreElements)
      throw new IllegalArgumentException("Input sequence is too large")

    val leftStart = coreMidPoint - seqLength / 2
    var count     = 0
    val data      = Array.ofDim[T]( numArrayElements )
    for (elem <- seq.take(seqLength)) {
      data(eltsIndex(leftStart + count)) = elem
      count += 1
    }

    if (count != seqLength)
      throw new IllegalArgumentException("There was not enough data in the seq")

    new LinBin[T]( emptyValue, data, leftStart, seqLength )
  }

  // Creates a new linbin from the data in arr(start).until(arr(start+length))
  //
  // Expects data to be sorted in accordance with ord. This is not checked and your responsibility
  //
  def fromSortedArray[T](emptyValue: T, arr: Array[T], start: Int, length: Int)
                        (implicit mf: Manifest[T], ord: Ordering[T]): LinBin[T] =
  {
    // We check this as it is an easy mistake, all other misalignment will turn into IndexOutOfBounds
    if (length > numCoreElements)
      throw new IllegalArgumentException("Input data is too large")

    val leftStart = coreMidPoint - length / 2
    var count     = 0
    val data      = Array.fill[T]( numArrayElements )( emptyValue )
    for (i <- start.until(start + length)) {
      data(eltsIndex(leftStart + count)) = arr(i)
      count += 1
    }

    new LinBin[T]( emptyValue, data, leftStart, length )
  }

  def fromSortedArray[T](emptyValue: T, arr: Array[T])(implicit mf: Manifest[T], ord: Ordering[T]): LinBin[T] =
    fromSortedArray(emptyValue, arr, 0, arr.length)


  final protected class LinBin[@specialized T](val emptyValue: T, protected val data: Array[T],
                                               dataStart: Int, dataLen: Int)
                                              (implicit mf: Manifest[T], ord: Ordering[T])
  {
    protected var start = dataStart
    protected var len   = dataLen

    def apply(value: T): T = {
      val corePos = dataSearch(value)
      if (corePos >= 0) {
        data(eltsIndex(corePos))
      } else {
        val stashPos = stashSearch(value, stashParents(eltsIndex(math.abs(corePos))))
        if (stashPos >= 0)
          data(stashPos)
        else
          emptyValue
      }
    }

    def contains(value: T): Boolean = ! ord.equiv(apply(value), emptyValue)

    def getOption(value: T): Option[T] = {
      val result = apply(value)
      if (ord.ne(result, emptyValue))
        Some(result)
      else
        None
    }

    // Adds value to this linbin
    //
    // Returns true, if that was possible, false if there was an equiv duplicate
    //
    // Throws StashOverflowException if the item could not be added due to stash overflow
    //
    @throws(classOf[be.bolder.hoodie.encore.linbin.StashOverflowException])
    def +=(value: T): Boolean = {
      val corePos = dataSearch(value, dataStart, dataLen)
      if (corePos >= 0) {
        false
      } else
        stashInsert(value, stashParents(eltsIndex(math.abs(corePos))))
    }

    // Finds item in the core using binary search
    //
    // Returns core index of found item or negative index of best match
    //
    @inline
    private def coreSearch(item: T, from: Int, len: Int): Int = {
      var index    = from
      var maxIndex = from + len -1

      while (index <= maxIndex) {
        val midIndex = index + ((maxIndex-index)/2)
        val midValue = data(eltsIndex(midIndex))

        val cmp      = ord.compare(item, midValue)

        if (cmp == 0) /* equiv */
          return midIndex
        else {
          if (cmp < 0) /* lt */
            maxIndex = midIndex - 1
          else
            index = midIndex + 1
        }
      }
      - index
    }

    // Finds item in the underlying data array using binary search
    //
    // Returns core index of found item or negative index of best match
    //
    @inline
    private def dataSearch(item: T, from: Int, len: Int): Int = {
      var index    = from
      var maxIndex = from + len -1

      while (index <= maxIndex) {
        val midIndex = index + ((maxIndex-index)/2)
        val midValue = data(midIndex)

        val cmp      = ord.compare(item, midValue)

        if (cmp == 0) /* equiv */
          return midIndex
        else {
          if (cmp < 0) /* lt */
            maxIndex = midIndex - 1
          else
            index = midIndex + 1
        }
      }
      - index
    }

    // Finds item in stash starting from stash startStash up to the top
    //
    // Returns core index if found, -1 otherwise
    //
    @inline
    private def stashSearch(item: T, startStash: Int): Int = {
      var parent = startStash

      while (parent != -1) {
        val limit = parent + stashNodeSize
        var i     = parent
        var cmp    = 0

        do {
          cmp = ord.compare(data(i), item)
          if (cmp == 0)
            return i
        } while ((i < limit) && (cmp > 0))
        parent = stashParents(parent)
      }
      -1
    }

    // Inserts item in stash starting from stash startStash unless it finds a duplicate
    //
    // Returns false, if a duplicate was found, true if item was inserted, and throws StashOverflowException
    // if there was not enough room to insert the item
    @inline
    @throws(classOf[be.bolder.hoodie.encore.linbin.StashOverflowException])
    private def stashInsert(item: T, startStash: Int): Boolean = {
      var parent      = startStash
      var cmp         = 0
      var emptyParent = -1
      var emptyIndex  = -1

      // search stash for item or first occurrence of an empty item, whatever comes first
      while ((parent != -1) && (emptyIndex < 0)) {
        val limit = parent + stashNodeSize
        var i     = parent
        do {
          val cur = data(i)
          cmp = ord.compare(item, cur)
          if (cmp == 0)
            return false

          if (ord.equiv(emptyValue, cur)) {
            emptyParent = parent
            emptyIndex  = i
          }
          i += 1
        } while ((i < limit) && (cmp < 0) && (emptyIndex < 0))
        parent = stashParents(parent)
      }

      // deal w. case that after the empty item the real item is in the current stash element
      if (emptyIndex >= 0) {
        var i     = emptyIndex + 1
        val limit = emptyParent + stashNodeSize
        if (i < limit) {
          do {
            val cur = data(i)
            cmp     = ord.compare(item, cur)
            if (cmp == 0)
              return false
            i += 1
          } while ((i < limit) && (cmp > 0))
        }
      }

      // keep searching for item in remaining stash elements
      while (parent != -1) {
        val limit = parent + stashNodeSize
        var i     = parent
        do {
          val cur = data(i)
          cmp = ord.compare(cur, item)
          if (cmp == 0)
            return false
          i+= 1
        } while ( ((i < limit)) && (cmp > 0))
        parent = stashParents(parent)
      }

      // We did not find the item in here

      // We don't have place to insert
      if (emptyIndex == -1)
        throw StashOverflowException
      else {
        // We do and thus insert it

        // For this, find the insertion point first
        val limit = parent + stashNodeSize
        var i     = emptyParent
        while (i < limit) {
          if ((ord.gt(data(i), item))) {
            if (emptyIndex < i) {
                Array.copy(data, emptyIndex + 1, data, emptyIndex, i - emptyIndex)
                data(i) = item
                return true
            } else if (emptyIndex > i) {
                Array.copy(data, i, data, i + 1, emptyIndex - i)
                data(i) = item
                return true
            }
          }
          i += 1
        }
      }
      // We should not get here
      throw new IllegalStateException("Unreachable")
    }
  }
}

object LinBinFactory {

  // This is optimized to get small values of linearCompares on the one hand (<50 or at least <100) but tries to keep
  // the number of levels small, too

  // How to read the constants defined below
  //
  // val STASH_S       = (A, B)
  //
  // Size(STASH_S)     = 2^(A+1) * 2^B
  // Compares(STASH_S) = A * 2^B
  //

  val STASH_32b  = ( 2, 2)  //  8
  val STASH_64b  = ( 2, 3)  // 16
  val STASH_128b = ( 3, 3)  // 24
  val STASH_256b = ( 3, 4)  // 48
  val STASH_512b = ( 4, 4)  // 64
  val STASH_1k   = ( 5, 4)  // 80
  val STASH_2k   = ( 6, 4)  // 96
  val STASH_4k   = ( 8, 3)  // 64
  val STASH_8k   = ( 9, 3)  // 72
  val STASH_16k  = (10, 3)  // 88
  val STASH_32k  = (11, 3)  // 96
  val STASH_64k  = (13, 2)  // 52
  val STASH_128k = (14, 2)  // 56
  val STASH_256k = (15, 2)  // 60

  val SIZE_64b   = 6
  val SIZE_128b  = 7
  val SIZE_256b  = 8
  val SIZE_512b  = 9
  val SIZE_1k    = 10
  val SIZE_2k    = 11
  val SIZE_4k    = 12
  val SIZE_8k    = 13
  val SIZE_16k   = 14
  val SIZE_32k   = 15
  val SIZE_64k   = 16
  val SIZE_128k  = 17
  val SIZE_256k  = 18
  val SIZE_512k  = 19
  val SIZE_1024k = 20

  lazy val Mini = new LinBinFactory(SIZE_64b, STASH_32b)

  lazy val Default5k = new LinBinFactory(SIZE_4k, STASH_1k)

  lazy val Default10k = new LinBinFactory(SIZE_8k, STASH_2k)

  lazy val Default20k = new LinBinFactory(SIZE_16k, STASH_4k)

  lazy val Default40k = new LinBinFactory(SIZE_32k, STASH_8k)

  lazy val Default80k = new LinBinFactory(SIZE_64k, STASH_16k)

  lazy val Default160k = new LinBinFactory(SIZE_128k, STASH_32k)

  lazy val Default320k = new LinBinFactory(SIZE_256k, STASH_64k)

  lazy val Default640k = new LinBinFactory(SIZE_512k, STASH_128k)

  lazy val Default1280k = new LinBinFactory(SIZE_1024k, STASH_256k)
}

object Test  {
  def main(args: Array[String]) {
    val factory = LinBinFactory.Mini

    val input   = Array.fill[Double](factory.numCoreElements)( math.random )
    Sorting.quickSort(input)

    val linbin  = factory.fromSortedArray(Double.NaN, input)

  }
}


