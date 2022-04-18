package com.databeans

import com.databeans.FlattenDataFrame.flattenDataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class Data (id:Long,father_name:String,names_sons:String,names_daugthers:String, father_name1:String,names_sons1:String,names_daugthers1:String)
class FlattenDataFrameSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("flattenDataFrame_Test")
    .getOrCreate()
  import spark.implicits._

  "flattenDataFrame" should "Flatten the json DataFrame" in {

    Given("The Json Dataframe  ")
    val df = spark.read.option("multiLine", true).json("input1.json") //todo :nesta3mel case class

    When("flattenDataFrame is invoked")
    val parsedFile = flattenDataFrame(df)
    df.printSchema()

    Then("the Flattened DataFrame should be returned")
    //val expectedResult = spark.read.option("multiLine", true).json("flatten.json") //todo : nesta3mel case class
    //expectedResult.as[Data].collect() should contain theSameElementsAs(parsedFile.as[Data].collect())

  }
}