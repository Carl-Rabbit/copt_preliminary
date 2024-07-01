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

    var cq2: Option[org.apache.spark.sql.DataFrame] = None

    withTimeStat("Whole") {
      withTimeStat("Q2") {
        val q2 = spark.sql("""
        |select * from a where a.x = 1 and a.y = 2
        """)
        q2.collect()
        cq2 = Some(q2)
      }
    }
    
    cq2.get.explain(true)
  }
}

try {
  Main.main(Array())
} catch {
  case e: Any => println(e)
}
System.exit(0)
