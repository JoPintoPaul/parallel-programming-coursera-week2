package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def balance(chars: Array[Char]): Boolean = {
    @tailrec
    def balanceWithTwoCounts(chars: List[Char], open: Int, closed: Int): Boolean = {
      if (chars.isEmpty) open == closed
      else {
        val (o, c) = chars.head match {
          case '(' => (open + 1, closed)
          case ')' => (open, closed + 1)
          case _ => (open, closed)
        }
        if (c > o) false
        else balanceWithTwoCounts(chars.tail, o, c)
      }
    }

    balanceWithTwoCounts(chars.toList, 0, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    @tailrec
    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      if (idx >= until) (arg1, arg2)
      else {
        val (o, c) = chars(idx) match {
          case '(' => (arg1 + 1, arg2)
          case ')' => (arg1, arg2 + 1)
          case _ => (arg1, arg2)
        }
        traverse(idx + 1, until, o, c)
      }
    }


    def reduce(from: Int, until: Int): (Int, Int) = {
//      val size = until - from
      until - from match {
        case size if size > threshold =>
          val splitIndex = size / 2
          val ((openOne, closeOne), (openTwo, closeTwo)) = parallel(
            reduce(from, from + splitIndex),
            reduce(from + splitIndex, until)
          )
          if (openOne > closeTwo) (openOne - closeTwo + openTwo, closeOne)
          else (openTwo, closeTwo - openOne + closeOne)
        case _ => traverse(from, until, 0, 0)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
