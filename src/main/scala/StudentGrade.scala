import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, max, mean, min, when}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
object StudentGrade {
  def main(args: Array[String]): Unit = {
    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "data")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val students = List(
      (1, "Alice", 92, "Math"),
      (2, "Bob", 85, "Math"),
      (3, "Carol", 77, "Science"),
      (4, "Dave", 65, "Science"),
      (5, "Eve", 50, "Math"),
      (6, "Frank", 82, "Science")
    ).toDF("student_id", "name", "score", "subject")


//   val df1  = students.select(col("student_id"), col("score"),
//      when(col("score") >= 90, "A")
//        .when(col("score") >= 80 && col("score") < 90, "B")
//        .when(col("score") >= 70 && col("score") < 80, "C")
//        .when(col("Score") >= 60 && col("score") < 70, "D")
//        .otherwise("F").as("grade")
//    )
//      students.groupBy(col("subject"))
//        .agg(
//          avg(col("score")).as("average_score"),
//          max(col("score")).as("maximum"),
//          min(col("score")).as("minimum")
//        ).show()
//
//        df1.groupBy(col("grade"))
//          .agg(
//            count(col("student_id")).as("Count")
//          ).show()

  students.createOrReplaceTempView("Student")

    spark.sql(
      """
      SELECT student_id ,score,
       CASE
        WHEN score >= 90 THEN 'A'
        WHEN score between 80 and 90 THEN 'B'
        WHEN score between 70 and 80 THEN 'C'
        WHEN score between 60 and 70 THEN 'D'
        ELSE 'F'
       END as grade
     FROM Student
        """
    ).show()

    spark.sql(
      """
      SELECT subject,
       count(student_id) as  count,
       CASE
        WHEN score >= 90 THEN 'A'
        WHEN score between 80 and 90 THEN 'B'
        WHEN score between 70 and 80 THEN 'C'
        WHEN score between 60 and 70 THEN 'D'
        ELSE 'F'
       END as grade
     FROM Student group by grade , subject
        """
    ).show()





    spark.sql(
      """
        SELECT subject, avg(score),
        min(score), max(score)
        FROM Student
        GROUP BY subject
        """).show()


  }
}




