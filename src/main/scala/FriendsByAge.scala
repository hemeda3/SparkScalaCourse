import org.apache.log4j._
import org.apache.spark._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object FriendsByAge {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../datasets/fakefriends1.csv")
    // ultimate result to get average of friends by age (grouped by age)
    // first def fun to extract map of (age,num of friends) only from each line
    // second group by age all friends
    // thierd find avge by devide number totla  of friends of groupedage by number of friends
    // thats mean we gotta keep track/record for number of friends

    val rdd=lines.map(parseLineToExtractAgeAndFriendsOnly)
    // add counter to preserve count of number of instance(use full to find mean/avg) = n values
    // convert numbOfFriends to tuple (#num,1)
     val groupAgeAndFriends=rdd.mapValues(x=>(x,1))
    // sum up total frie and total instance for each age(key)
      val sumValuesOfFriendsForcommandAgeGroupBY=groupAgeAndFriends.
        reduceByKey( (x,y) => (x._1+y._1,x._2+y._2) )
    // find avg by devide total friends no divided by number of instance
     val avgFriendsByAge= sumValuesOfFriendsForcommandAgeGroupBY.mapValues(x=>x._1/x._2)

    val result=avgFriendsByAge.collect()
    result.sorted.foreach(println)

  }

  def  parseLineToExtractAgeAndFriendsOnly(line :String)  ={
      // split by comma
      val arrayOfAllColumnsFileds=line.split(",")
      // extract the age  and numFriends and fields, and convert to Integer
      val age= arrayOfAllColumnsFileds(2).toInt
      val friends= arrayOfAllColumnsFileds(3).toInt
    (age,friends)

  }
}
