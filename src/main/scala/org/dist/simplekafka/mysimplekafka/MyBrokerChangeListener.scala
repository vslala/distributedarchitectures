package org.dist.simplekafka.mysimplekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.utils.ZkUtils.Broker

case class MyBrokerChangeListener() extends IZkChildListener {
  import scala.jdk.CollectionConverters._
  
  var currIds: Set[Int] = Set()
  
  override def handleChildChange(parentPath: String, currentBrokerList: util.List[String]): Unit = {
    currIds = currentBrokerList.asScala.map(_.toInt).toSet
  }
  
  def getCurrentIds(): Set[Int] = {
    return currIds
  }
}
