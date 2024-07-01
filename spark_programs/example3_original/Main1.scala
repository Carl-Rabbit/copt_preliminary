object Main {
  def withTimeStat(name: String)(f: => Unit): Unit = {
    val start = System.currentTimeMillis()
    f
    val end = System.currentTimeMillis()
    println(f"$name: ${(end - start) / 1000f} s")
  }

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("trace")
    import spark.implicits._

    val size = 500000
    val a = List.range(0, size).zip(List.range(0, size)).toDF("x", "y").cache()
    val b = List.range(size / 2, size + size / 2).zip(List.range(1, size + 1)).toDF("x", "z").cache()
    a.createOrReplaceTempView("a")
    b.createOrReplaceTempView("b")

    var cq1: Option[org.apache.spark.sql.DataFrame] = None

    withTimeStat("Whole") {
      withTimeStat("Q1") {
        val q1 = spark.sql("""
        |select * from a join b on a.x = b.x
        |where a.y = 1
        """)
        q1.collect()
        cq1 = Some(q1)
      }
    }

    cq1.get.explain(true)
  }
}

try {
  Main.main(Array())
} catch {
  case e: Any => println(e)
}
System.exit(0)
