package day13

object tountitl {
  def main(args: Array[String]): Unit = {
    val value1 = 1 until  10
    println(value1)
    val value2 = 1 to 10
    println(value2)
    val value3 = 1 to 10 by 2
    println(value3)
    val value4 = 1 until 10 by 2
    println(value4)
  }
}
