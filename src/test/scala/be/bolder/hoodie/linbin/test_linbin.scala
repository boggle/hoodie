package be.bolder.hoodie.linbin

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
  }
}
