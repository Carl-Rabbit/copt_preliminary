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
    import org.apache.spark.sql.SaveMode

    //val size = 1000000
    //List.range(1, size).zip(List.range(1, size)).toDF("x", "y").write.mode(SaveMode.Overwrite).saveAsTable("a")
    //List.range(1, size).toDF("y").write.mode(SaveMode.Overwrite).saveAsTable("c")

    var cq1: Option[org.apache.spark.sql.DataFrame] = None
    var cq2: Option[org.apache.spark.sql.DataFrame] = None

    withTimeStat("Whole") {
      withTimeStat("Q1") {
        val q1 = spark.sql("""
        |select * from a where a.x = 1
        """)
        q1.collect()
        cq1 = Some(q1)
      }

      withTimeStat("Q2") {
        val q2 = spark.sql("""
        |with b as (select * from a where a.x = 1)
        |select * from b join c on b.y = c.y
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
