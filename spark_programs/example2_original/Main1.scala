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

    val size = 100000
    val a = List.range(1, size).zip(List.range(2, size + 1)).toDF("x", "y").cache()
    a.createOrReplaceTempView("a")

    var cq1: Option[org.apache.spark.sql.DataFrame] = None

    withTimeStat("Whole") {
      withTimeStat("Q1") {
        val q1 = spark.sql("""
        |select * from a where a.x = 1
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
