package com.demo.people

import java.io.FileWriter
import java.io.File
import scala.util.Random

/**
 *  生成测试数据
 */
object PeopleInfoFileGenerator {
  def main(args: Array[String]) {
    val writer = new FileWriter(new File("E:\\BigData\\data\\input\\sample_people_info.txt"), false)
    val rand = new Random()
    for (i <- 1 to 100000) {
      var height = rand.nextInt(220)
      if (height < 50) {
        height = height + 50
      }
      var gender = getRandomGender
      if (height < 100 && gender == "M") // 男生
        height = height + 100
      if (height < 100 && gender == "F") // 女生
        height = height + 50
      // 序号，性别，身高
      writer.write(i + " " + getRandomGender + " " + height + "\n")
    }
    writer.flush()
    writer.close()
    println("People Information File generated successfully.")
  }

  def getRandomGender(): String = {
    val rand = new Random()
    val randNum = rand.nextInt(2) + 1
    if (randNum % 2 == 0) {
      "M"
    } else {
      "F"
    }
  }
}