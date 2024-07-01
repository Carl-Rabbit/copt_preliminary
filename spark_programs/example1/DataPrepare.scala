import scala.util.Random

object Main {
  def withTimeStat(name: String)(f: => Unit): Unit = {
    val start = System.currentTimeMillis()
    f
    val end = System.currentTimeMillis()
    println(f"$name: ${(end - start) / 1000f} s")
  }

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    sc.setLogLevel("WARN")

    val database = "ex1"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS $database")
    spark.sql(f"USE $database")

    spark.sql(f"DROP TABLE IF EXISTS a")
    spark.sql(f"CREATE TABLE IF NOT EXISTS a (id INT, x INT, y INT)")
    spark.sql(f"DROP TABLE IF EXISTS b")
    spark.sql(f"CREATE TABLE IF NOT EXISTS b (id INT, z INT, w INT)")

    withTimeStat("Prepare data") {
      val random = new Random()

      val size = 1000000
      val max = 1000
      val batchCnt = 50

      for (batchIdx <- 0 until batchCnt) {
        var stat = f"INSERT INTO a VALUES "
        val start = batchIdx * size / batchCnt
        val end = (batchIdx + 1) * size / batchCnt
        for (i <- start until end) {
          val x = random.nextInt(max)
          val y = random.nextInt(max)
          stat += f"($i,$x,$y),"
        }
        spark.sql(stat.dropRight(1))
        println(f"$end/$size")
      }

      for (batchIdx <- 0 until batchCnt) {
        var stat = f"INSERT INTO b VALUES "
        val start = batchIdx * size / batchCnt
        val end = (batchIdx + 1) * size / batchCnt
        for (i <- start until end) {
          val x = random.nextInt(max)
          val y = random.nextInt(max)
          stat += f"($i,$x,$y),"
        }
        spark.sql(stat.dropRight(1))
        println(f"$end/$size")
      }
    }
  }
}

try {
  Main.main(Array())
} catch {
  case e: Any => println(e)
}
System.exit(0)
