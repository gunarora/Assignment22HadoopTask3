package SparkSql

import org.apache.spark.sql.SparkSession

/**
  * Created by garora19 on 5/28/2018.
  */
object SparkSQLUseCase1 {
  case class hvac_cls(Date:String,Time:String,TargetTemp:Int,ActualTemp:Int,System:Int,SystemAge:Int,BuildingId:Int)
  case class building(buildid:Int,buildmgr:String,buildAge:Int,hvacproduct:String,Country:String)
  def main(args: Array[String]): Unit = {
    println("hey scala");
    //Let us create a spark session object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL Use Cae 1")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    println("Spark Session Object created")

    val data = spark.sparkContext.textFile("C:\\Users\\garora19\\Desktop\\Hadoop\\Spark\\Spark Syed\\Spark SQL Case Studies\\HVAC.csv")

    println("HVAC Data->>" + data.count)

    val header = data.first()
    val data1 = data.filter(row => row != header)
    println("Header removed from the data !");


    //For implicit conversions like converting RDDs and sequences  to DataFrames
    import spark.implicits._

    val hvac = data1.map(x=>x.split(",")).map(x => hvac_cls(x(0),x(1),x(2).toInt,x(3).toInt,x(4).toInt,x(5).toInt,x(6).toInt)).toDF()

    hvac.show()
    println("HVAC Dataframe created !")
    hvac.registerTempTable("HVAC")

    println("Dataframe Registered as table !")

    val hvac1 = spark.sql("select *,IF((targettemp - actualtemp) > 5, '1', IF((targettemp - actualtemp) < -5, '1', 0)) AS tempchange from HVAC")

    hvac1.show()

    hvac1.registerTempTable("HVAC1")

    println("Data Frame Registered as HVAC1 table !")

    //Now lets load the second data set

    val data2 = spark.sparkContext.textFile("C:\\Users\\garora19\\Desktop\\Hadoop\\Spark\\Spark Syed\\Spark SQL Case Studies\\building.csv");

    val header1 = data2.first()

    val data3 = data2.filter(row => row != header1)


    println("Header removed from the building data")

    //Now let us create the building dataframe

    val build = data3.map(x=> x.split(",")).map(x => building(x(0).toInt,x(1),x(2).toInt,x(3),x(4))).toDF

    build.show()

    build.registerTempTable("building")

    println("Buildings data registered as building table")

    //Now join the two tables
    val build1 = spark.sql("select h.*, b.country, b.hvacproduct from building b join hvac1 h on b.buildid = h.buildingid")
    build1.show()

    //Select temperature and country column from above

    val tempCountry = build1.map(x => (new Integer(x(7).toString),x(8).toString))
    tempCountry.show()

    //Filter the values

    val tempCountryOnes = tempCountry.filter(x=> {if(x._1==1) true else false})
    tempCountryOnes.groupBy("_2").count.show

    //Save the output to the disk
    //tempCountryOnes.write.save("/Users/syed/sparksqloutput")

  }
}
