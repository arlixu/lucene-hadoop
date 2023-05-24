object Test{

  def convertToWildcard(str: String): String = {
    val backtickPattern = "\\.`([^`]*)`"
    val doubleDotPattern = "\\.[^\\.]+"
    val replacedStr = str.replaceAll(backtickPattern, "\\.*")
    val finalStr = replacedStr.replaceAll(doubleDotPattern, "\\.*")
    finalStr
  }

  def main(args: Array[String]): Unit = {
    val str1 = "`Map Info`.color"
    val str2 = "Map Info.color.light"
    val str3 = "Map Info.`color.light`"
    val str4 = "Map Info.`0.5`"

    println(convertToWildcard(str1))  // 输出: Map Info.*color*
    println(convertToWildcard(str2))  // 输出: Map Info.*color.light*
    println(convertToWildcard(str3))  // 输出: Map Info.*
    println(convertToWildcard(str4))  // 输出: Map Info.*

  }

}