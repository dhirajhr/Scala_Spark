import java.io.{File, PrintWriter}

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

object Task3 {

  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("StackOverflow").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //val writer = new PrintWriter(new File("src/main/Output/Dhiraj_Ramnani_task3.csv"))
    val writer = new PrintWriter(new File(args(1)))

    sc.setLogLevel("ERROR")

    val df = sqlContext.read
      .format("csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(args(0))
      //.load("src/main/resources/survey_results_public.csv")
    val col_names=Seq("Country","Salary","SalaryType")
    val df_new = df.select(col_names.head,col_names.tail:_*).filter("Salary!='NA'").filter("Salary!='0'")
    //Task
    val rows: RDD[Row] = df_new.rdd

    val countValues_map=rows.map(row=>(row(0).toString,1))
    val countValues=countValues_map.reduceByKey(_ + _)

    //Task 3
    //val tmp=rows.map(row=>(row(0).toString,row(1).toString.replaceAll(",","").toDouble))
    val tmp=rows.map(row=>(row(0).toString, if (row(2) == "Monthly") row(1).toString.replaceAll(",","").toDouble * 12 else if (row(2) == "Weekly") row(1).toString.replaceAll(",","").toDouble * 52 else row(1).toString.replaceAll(",","").toDouble))
    val maxValues=tmp.reduceByKey(math.max(_, _))
    val minValues=tmp.reduceByKey(math.min(_, _))
    val avgValues=tmp.map{ case(country: String, salary: Double) => (country, (salary, 1)) }
      .reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2) ).mapValues{ case (sum, count) => (1.0 * sum) / count }
    val cnt=countValues.join(maxValues)
    val max_min=cnt.join(minValues)
    val avg_joined=max_min.join(avgValues)
    val avg_res=avg_joined.sortByKey(true)
    val final_res=avg_res.map{case (country,(((count,max),min),avg))=>(country,count,min,max,avg)}
    //final_res.take(5).foreach(println)

    val file_write=final_res.map{case(country,count,min,max,avg) =>
      if(country.contains(",")) {
        var line = '"' + country.toString + '"' + "," + count.toString + "," + min.toInt.toString + "," + max.toInt.toString + "," + "%.2f".format(avg).toDouble + "\n"
        line
      }
        else
        {
          var line = country.toString + "," + count.toString + "," + min.toInt.toString + "," + max.toInt.toString + "," + "%.2f".format(avg).toDouble + "\n"
          line
        }
    }
    val list = file_write.collect().toList
    list.foreach(writer.write)
    writer.close()

  }


}
