package be.bolder.hoodie.linbin

import compat.Platform
import java.lang.IllegalStateException
import util.Sorting
import collection.mutable.ArrayOps
import java.util.Arrays

object SmokeTest {
  def main(args: Array[String]) {
    val data    = 0.to(30, 2).toArray
    val linbin1 = LinBin.fromItemSortedArray[Int](4, 2, 1, data)

    for (value <- data) {
      System.out.println(value)
      assert(linbin1.contains(value))
    }

    assert(! linbin1.contains(1))

    linbin1 += -2

    assert( linbin1.contains(-2))

    linbin1 += -3
    linbin1 += -4
    linbin1 += -5

    var ok = false
    try {
      linbin1 += -6
    }
    catch {
      case (_: StashOverflowException) => ok = true
    }
    assert(ok)

    linbin1 -= -5
    linbin1 += -1

    linbin1 += 9

    System.out.println("Check")


    val times = 100000
    val input = Array.fill[Double](4000000)(math.random)(Manifest.Double)
    Sorting.quickSort(input)



    {
      System.out.println("plain binsearch")

      val start  = Platform.currentTime
      for (t <- 0.until(times)) {
        val inputIndex = (math.random * input.length).toInt
        val inputValue = input(inputIndex)
        val result = input(Arrays.binarySearch(input, inputValue))
        if (result != inputValue)
          throw new IllegalStateException("Retrieval error")
      }
      val stop  = Platform.currentTime
      System.out.println(stop-start)
    }

    {
      System.out.println("empty linbin")

      val linbin = LinBin.fromItemSortedArray[Double](22, 6, 11, input)
      val start  = Platform.currentTime
      for (t <- 0.until(times)) {
        val inputIndex = (math.random * input.length).toInt
        val inputValue = input(inputIndex)
        val result = linbin( inputValue )
        if (result != inputValue)
          throw new IllegalStateException("Retrieval error")
      }
      val stop  = Platform.currentTime
      System.out.println(stop-start)
    }

  }
}
