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

    var cq1: Option[org.apache.spark.sql.DataFrame] = None
    var cq2: Option[org.apache.spark.sql.DataFrame] = None

    withTimeStat("Whole") {
      withTimeStat("T1") {
        val t1 = spark.sql("""
        |create temp view _temp as 
        |select * from a where a.x = 1
        """)
        t1.collect()
      }

      withTimeStat("Q1") {
        val q1 = spark.sql("""
        |select * from _temp
        """)
        q1.collect()
        cq1 = Some(q1)
      }

      withTimeStat("Q2") {
        val q2 = spark.sql("""
        |select * from _temp join c on _temp.y = c.y
        """)
        q2.collect()
        cq2 = Some(q2)
      }
    }

    cq1.get.explain(true)
    cq2.get.explain(true)
  }
}

try {
  Main.main(Array())
} catch {
  case e: Any => println(e)
}
System.exit(0)
