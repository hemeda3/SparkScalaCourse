import org.apache.spark.SparkContext

object MinTempratureByLocation {

  def main(args: Array[String]){
    val sc= new SparkContext("local[*]","MinTemprateByLocation")
    val lines = sc.textFile("../datasets/1800.csv")
    // clean fields to only wanted 3 fields
    val rdd=lines.map(parseLines)
    // filter min status = TMIN and degree is the minimum
    val filteredByMinStatus=rdd.filter(x=>x._2.eq("TMIN"))
    val fitlerRDDByFindMin=rdd.filter(x=>x._3)

  }
  def parseLines(line : String) = {

    val fields=line.split(",")
    val location=fields(1)
    val tempStatus=fields(3)
    val tempdegree=fields(4).toInt

    (location,tempStatus,tempdegree)
  }
}
