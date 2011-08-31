package be.bolder.hoodie.linbin

import compat.Platform
import java.lang.IllegalStateException
import util.Sorting
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

    val trials = 5
    for (_ <- 0.until(trials)) {

      val times = 1000000
      val input = Array.fill[Double](8000000)(math.random)(Manifest.Double)
      Sorting.quickSort(input)



      {
        System.out.println("full stash linbin")

        val linbin = LinBin.fromItemSortedArray[Double](23, 4, 10, input)
        linbin.simulateFullStash
        val start  = Platform.currentTime
        for (t <- 0.until(times)) {
          val inputIndex = (math.random * input.length).toInt
          val inputValue = input(inputIndex) + 0.000000000000001d
          val result = linbin.getDefault( inputValue, 0.0d )
        }
        val stop  = Platform.currentTime
        System.out.println(stop-start)
      }

      {
        System.out.println("empty linbin")

        val linbin = LinBin.fromItemSortedArray[Double](23, 1, 2, input)
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

    }
  }
}
