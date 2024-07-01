object Main {
  def withTimeStat(name: String)(f: => Unit): Unit = {
    val start = System.currentTimeMillis()
    f
    val end = System.currentTimeMillis()
    println(f"$name\n${(end - start) / 1000f} s")
  }

  def exec(sql: String): Unit = {
    var cq: Option[org.apache.spark.sql.DataFrame] = None

    withTimeStat(f"[SQL] $sql") {
      val q = spark.sql(sql)
      q.collect
      cq = Some(q)
    }

    cq.get.explain(true)
  }

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("trace")
    import spark.implicits._

    val database = "ex1"
    spark.sql(f"USE $database")

    exec("cache table temp as select * from a where a.x = 1")
    exec("select * from temp")
    exec("select * from temp where temp.y = 2")
  }
}

try {
  Main.main(Array())
} catch {
  case e: Any => println(e)
}
System.exit(0)
