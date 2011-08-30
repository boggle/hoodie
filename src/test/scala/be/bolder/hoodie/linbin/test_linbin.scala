package be.bolder.hoodie.linbin

object Test {
  def main(args: Array[String]) {
    val data    = 0.to(30, 2).toArray
    val linbin1 = LinBin.fromItemSortedArray[Int](4, 2, 1, data)

    for (value <- data) {
      System.out.println(value)
      assert(linbin1.contains(value))
    }

    assert(! linbin1.contains(1))

    linbin1 += -1

    assert( linbin1.contains(-1))

    linbin1 += -2
    linbin1 += -3
    linbin1 += -4
    linbin1 += -5

    System.out.println("Check")
  }
}
