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

    exec("select * from a where x = 1")
    exec("select * from a where x = 1 and y = 2")
    exec("select * from (select * from a where x = 1) where y = 2")

    // spark.sql("select * from a where x = 1").cache.collect
    // spark.sql("select * from a where x = 1 and y = 2").collect
    // spark.sql("select * from (select * from a where x = 1) where y = 2").collect
  }
}

try {
  Main.main(Array())
} catch {
  case e: Any => println(e)
}
System.exit(0)
