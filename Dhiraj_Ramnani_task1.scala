import java.io.{File, PrintWriter}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

object Task1 {

  def main(args : Array[String]): Unit = {

    var conf = new SparkConf().setAppName("StackOverflow").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //val writer = new PrintWriter(new File(args(1)))
    val writer = new PrintWriter(new File(args(1)))
    sc.setLogLevel("ERROR")

    val df = sqlContext.read
      .format("csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(args(0))

    val col_names=Seq("Country","Salary","SalaryType")
    val df_new = df.select(col_names.head,col_names.tail:_*).filter("Salary!='NA'").filter("Salary!='0'")
    val rows: RDD[Row] = df_new.rdd
    val totalValues=rows.count()
    val rdd1=sc.parallelize(Seq(("Total", totalValues))) //rdd1
    val countValues=rows.map(row=>(row(0).toString,1.toLong)).reduceByKey(_ + _).sortByKey(true)
    val final_res=rdd1.union(countValues)

    val file_write=final_res.map{case(country,count) =>
      if(!country.contains(',')) {
        var line = country.toString + "," + count.toString + "\n"
        line
      }
        else
        {
          var line = '"'+ country.toString + '"' + "," + count.toString + "\n"
          line
        }
    }
    val list = file_write.collect().toList
    list.foreach(writer.write)
    writer.close()

  }


}
