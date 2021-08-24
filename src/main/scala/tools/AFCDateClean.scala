package tools

class AFCDateClean extends VariousDateOperations with Serializable {
  override def standardDateConversion(dateString: String): String = {
    val dateAndTimeArray = dateString.split(" ")
    val dateArray = dateAndTimeArray(0).split("/")
    val time = dateAndTimeArray(1)
    val year = dateArray(0)
    val month = dateArray(1)
    val addZero = (x: String) => {
      if (x.toInt < 10) {
        val y = String.format("0%s", x)
        y
      } else {
        x
      }
    }
    val day = dateArray(2)
    String.format("%s-%s-%s %s", year, addZero(month), addZero(day), time)
  }
}
