# Scala_Spark

We will run the scala code as follows:

$SPARK_HOME/bin/spark-submit -- class Task1 Dhiraj_Ramnani.jar <input_path> <output_path> <br/>
$SPARK_HOME/bin/spark-submit -- class Task2 Dhiraj_Ramnani.jar <input_path> <output_path> <br/>
$SPARK_HOME/bin/spark-submit -- class Task3 Dhiraj_Ramnani.jar <input_path> <output_path> <br/><br/>

Note: <input_path>: Path to Stackoverflow Dataset <br/>

Task 1: compute the total number of survey responses per country that have
provided a salary value – i.e., response entries containing ‘ NA ’ or ‘ 0 ’ salary values are considered
non-useful responses and should be discarded. <br/><br/>

Task 2: Partition function (using the country value as driver) to improve the performance
of map and reduce tasks. A time span comparison between the standard (RDD used in Task 1)
and partition (RDD built using the partition function). <br/><br/>

Task 3: Compute annual salary averages per country and show min and max
salaries. <br/><br/>

