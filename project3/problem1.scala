import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "Transactions.csv", "header" -> "false"))

val T1 = df.filter(df("_c2")>=200)

val T2 = T1.groupBy("_c3").agg(sum(T1("_c2")),avg(T1("_c2")),max(T1("_c2")),min(T1("_c2")))
T2.show()

val T3 = T1.groupBy("_c1").agg(count(T1("_c0")) as "num1")
T3.show()

val T4 = df.filter(df("_c2")>=600)

val T5 = T4.groupBy($"_c1" as "_c1_2").agg(count(T4("_c0")) as "num2")

val Temp = T3.join(T5,T3("_c1") === T5("_c1_2"))
val T6 = Temp.select(Temp("_c1")).filter(Temp("num1")> Temp("num2")*3) 
T6.show()
