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

    val size = 50000
    val a = List.range(1, size).zip(List.range(1, size)).toDF("x", "y").cache()
    val c = List.range(1, size).toDF("y").cache()
    a.createOrReplaceTempView("a")
    c.createOrReplaceTempView("c")

    var cq2: Option[org.apache.spark.sql.DataFrame] = None

    withTimeStat("Whole") {
      withTimeStat("Q2") {
        val q2 = spark.sql("""
        |with b as (select * from a where a.x = 1)
        |select * from b join c on b.y = c.y
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
