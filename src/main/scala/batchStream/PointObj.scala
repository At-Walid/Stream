package org.clustering4ever.spark.clustering

import java.io.Serializable

import breeze.linalg.Vector
/**
  * Copyright: please refer to the README.md file
  * User: ATTAOUI & Ghesmoune
  * Date: 28/03/2020
  * */


final case class PointObj(
  var pointPartNum: Vector[Double],//the numeric part of the data-point
  val label: Int, //the real (provided) label (it is not used in the learning but for visualization and measuring performance criteria)
  val id: Int     //the identifier(=the line number) of the data-point
) extends Serializable {
/**
 * @param pointPartNum the numeric part of the data-point
 * @param label the real (provided) label (it is not used in the learning but for visualization and measuring performance criteria)
 * @param id the identifier(=the line number) of the data-point
 */


  override def toString: String = {
    pointPartNum.toArray.deep.mkString(", ")
  }

}
class streamData (val stream : Array[PointObj])
  extends Serializable {

  override def toString: String = {
    stream.deep.mkString(", ")
  }
}
/**
 *
 * @param protoPartNum
 * @param idsDataAssigned
 * @param id
 */
final case class Prototype(
  var protoPartNum: Vector[Double],
  var idsDataAssigned : Set[Int],
  val id: Int
) extends Serializable {
  override def toString: String = {toStringProto
    "node: "+id +" -> " + protoPartNum.toArray.deep.mkString(", ")
  }

  def toStringIds: String = {

    "node: " + id + " (" + idsDataAssigned.size + " data-points)" + " -> "  + idsDataAssigned.toArray.deep.mkString(", ")
  }
  
  def toStringProto: String = {
    protoPartNum.toArray.deep.mkString(", ")
  }

  def toStringCard: String = {
    idsDataAssigned.size.toString()
  }
  
  def toStringAss: String = {
    idsDataAssigned.toArray.deep.mkString(", ")
  }
  
  def toStringId: String = {
    id.toString()
  }

}
