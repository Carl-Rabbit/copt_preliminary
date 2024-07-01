object Main {
  def withTimeStat(name: String)(f: => Unit): Unit = {
    val start = System.currentTimeMillis()
    f
    val end = System.currentTimeMillis()
    println(f"$name: ${(end - start) / 1000f} s")
  }

  def exec(sql: String, cache: Boolean = false): Unit = {
    var cq: Option[org.apache.spark.sql.DataFrame] = None

    withTimeStat("[TIME]") {
      val q = spark.sql(sql)
      q.collect
      if (cache) {
        q.cache
      }
      cq = Some(q)
    }

    cq.get.explain(true)
  }

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("warn")
    import spark.implicits._

    val database = "ex1"
    spark.sql(f"USE $database")

    exec("select * from a where x = 1", true)
    exec("select * from (select * from a where x = 1) where y = 2")
  }
}

try {
  Main.main(Array())
} catch {
  case e: Any => println(e)
}
System.exit(0)
