// Problem 2
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD




def cellID(input:List[String]):Int={
   // val s:List[String]=input.split(",")
    val x=((input(0).toDouble)/20).toInt+1
    val y=499-((input(1).toDouble)/20).toInt
    val index=x+500*y
    return index
  }


def getNeighbors(input:Int):Array[Int]={
   
    val index= input
    val x = index%500
    val y = (index-x)/500
    val list = ListBuffer[Int](index+1,index-1,index+500,index-500,index+501,index-501,index+499,index-499)
    if(x<=1){
      list-=(index-1,index-501,index+499)
    }
    if(x>=500){
      list-=(index+1,index+501,index-499)
    }
    if(y<=0){
      list-=(index-500,index-501,index-499)
    }
    if(y>=499){
      list-=(index+500,index+501,index+499)
    }
    return list.toList.toArray
  }






	val df = sc.textFile("file:///home/mqp/Documents/proj3/points.csv")

	val points = df.map(line => line.split(",").toList)

	val cells = points.map(coord => (cellID(coord), 1)).reduceByKey(_ + _).collect()

	val c2 = cells.map(p => (p._2, p._1))
	val c3 = sc.parallelize(c2)
	val c4 = c3.flatMapValues(p => getNeighbors(p))
	val c5 = c4.map(p => (p._2, p._1))
	val c6 = c5.mapValues((_,1)).reduceByKey((x,y) => (x._1+y._1,x._2+y._2)).mapValues(p => p._1/p._2).collectAsMap()

	// map of (cell, relative density)
	val c7 = cells.map(p => (p._1,p._2.toFloat/c6(p._1).toFloat))

	val c8 = sc.parallelize(c7)

	// Report the top 50 according to their RDI
	val result = c8.sortBy(-_._2).take(50)

	sc.parallelize(result,1).saveAsTextFile("output")

	val r2 = result.map(p => (p._1, getNeighbors(p._1).map(p => (p, c7(p)._2)).toList))

	sc.parallelize(r2, 1).saveAsTextFile("output2")

	
	

	

