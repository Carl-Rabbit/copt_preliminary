object Main {
  def withTimeStat(name: String)(f: => Unit): Unit = {
    val start = System.currentTimeMillis()
    f
    val end = System.currentTimeMillis()
    println(f"$name: ${(end - start) / 1000f} s")
  }

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("warn")
    import spark.implicits._

    val database = "ex1"
    spark.sql(f"USE $database")

    var cq1: Option[org.apache.spark.sql.DataFrame] = None

    withTimeStat("[TIME]") {
      val q1 = spark.sql("""
      |with c as (select * from a where a.x = 1)
      |select * from c join b on c.y = b.w
      """)
      q1.collect
      cq1 = Some(q1)
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
