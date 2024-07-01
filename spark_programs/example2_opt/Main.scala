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

    spark.sql("use database ex2").collect

    //val size = 1000000
    //List.range(0, size).zip(List.range(1, size + 1)).toDF("x", "y").write.mode(SaveMode.Overwrite).saveAsTable("a")

    var cq1: Option[org.apache.spark.sql.DataFrame] = None
    var cq2: Option[org.apache.spark.sql.DataFrame] = None

    spark.sql("""
    |uncache table if exists _temp
    """).collect

    withTimeStat("Whole") {
      withTimeStat("T0") {
        val t0 = spark.sql("""
        |cache table _temp as
        |select * from a where a.x = 1
        """)
        t0.collect()
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
        |select * from _temp where _temp.y = 2
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
