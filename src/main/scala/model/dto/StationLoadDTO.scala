package model.dto

case class StationLoadDTO(stationId: String, avgVolume: Double, inVolume: Double,
                          outVolume: Double, passengers: Double, crowdingRate: Double) {

}
