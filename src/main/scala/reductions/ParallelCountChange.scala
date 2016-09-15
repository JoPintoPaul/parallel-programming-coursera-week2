package reductions

import org.scalameter._
import common._

object ParallelCountChangeRunner {

  @volatile var seqResult = 0

  @volatile var parResult = 0

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 20,
    Key.exec.maxWarmupRuns -> 40,
    Key.exec.benchRuns -> 80,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val amount = 250
    val coins = List(1, 2, 5, 10, 20, 50)
    val seqtime = standardConfig measure {
      seqResult = ParallelCountChange.countChange(amount, coins)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential count time: $seqtime ms")

    def measureParallelCountChange(threshold: ParallelCountChange.Threshold): Unit = {
      val fjtime = standardConfig measure {
        parResult = ParallelCountChange.parCountChange(amount, coins, threshold)
      }
      println(s"parallel result = $parResult")
      println(s"parallel count time: $fjtime ms")
      println(s"speedup: ${seqtime / fjtime}")
    }

    measureParallelCountChange(ParallelCountChange.moneyThreshold(amount))
    measureParallelCountChange(ParallelCountChange.totalCoinsThreshold(coins.length))
    measureParallelCountChange(ParallelCountChange.combinedThreshold(amount, coins))
  }
}

object ParallelCountChange {

  /** Returns the number of ways change can be made from the specified list of
    * coins for the specified amount of money.
    */
  def countChange(money: Int, coins: List[Int]): Int = {
    def count(m: Int, c: List[Int], found: Int): Int = {
      (m, c.length) match {
        case end if m < 0 => found
        case (0, 0) => found + 1
        case (_, 0) => found
        case _ => count(m, c.tail, found) + count(m - c.head, c, found)
      }
    }

    money match {
      case lessThanZero if money < 0 => 0
      case 0 => 1
      case _ => count(money, coins, 0)
    }
  }

  type Threshold = (Int, List[Int]) => Boolean

  /** In parallel, counts the number of ways change can be made from the
    * specified list of coins for the specified amount of money.
    */
  def parCountChange(money: Int, coins: List[Int], threshold: Threshold): Int = {
    (threshold(money, coins), money, coins.length) match {
      case (true, _, _) => countChange(money, coins)
      case (_, _, 0) => countChange(money, coins)
      case zeroMoney if money <= 0 => countChange(money, coins)
      case _ => {
        val (left, right) =
          parallel(
            parCountChange(money - coins.head, coins, threshold),
            parCountChange(money, coins.tail, threshold)
          )
        left + right
      }
    }
  }

  /** Threshold heuristic based on the starting money. */
  def moneyThreshold(startingMoney: Int): Threshold =
    (money: Int, coins: List[Int]) => ((startingMoney * 2) / 3) >= money

  /** Threshold heuristic based on the total number of initial coins. */
  def totalCoinsThreshold(totalCoins: Int): Threshold = {
    (money: Int, coins: List[Int]) => ((totalCoins * 2) / 3) >= coins.length
  }

  /** Threshold heuristic based on the starting money and the initial list of coins. */
  def combinedThreshold(startingMoney: Int, allCoins: List[Int]): Threshold = {
    (money: Int, coins: List[Int]) =>
      (money * coins.length) <= ((startingMoney * allCoins.length) / 2)
  }
}
