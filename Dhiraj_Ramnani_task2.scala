import java.io.{File, PrintWriter}

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

object Task2 {

  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("StackOverflow").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //val writer = new PrintWriter(new File("src/main/Output/Dhiraj_Ramnani_task2.csv"))
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
    val countValues_map=rows.map(row=>(row(0),1))
    val st1 = System.currentTimeMillis()
    val countValues=countValues_map.reduceByKey(_ + _).collect()
    val et1 = System.currentTimeMillis()
    val countValues_part_map=countValues_map.partitionBy(new HashPartitioner(2))//.cache
    val st2 = System.currentTimeMillis()
    val countValues_part=countValues_part_map.reduceByKey(_+_).collect()
    val et2 = System.currentTimeMillis()

    //countValues_part_map.mapPartitionsWithIndex{ case (i, rows) => Iterator((i, rows.size))}
    val without_part=countValues_map.mapPartitions(iter => Array(iter.size).iterator, true).collect()
    val with_part=countValues_part_map.mapPartitions(iter => Array(iter.size).iterator, true).collect()
    val final_res="standard,"+without_part(0).toString+","+without_part(1).toString+","+(et1-st1).toString+"\n"+"partition,"+with_part(0).toString+","+with_part(1).toString+","+(et2-st2).toString

    //println(et1-st1)
    //println(et2-st2)
    writer.write(final_res)
    writer.close()

  }



}
