package org.dist.simplekafka.mysimplekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener

case class MyBrokerChangeListener(zookeeperClient: MyZookeeperClient) extends IZkChildListener {
  import scala.jdk.CollectionConverters._
  
  override def handleChildChange(parentPath: String, currentBrokerList: util.List[String]): Unit = {
    zookeeperClient.liveBrokers = currentBrokerList.asScala.map(_.toInt).toSet
  }
  
  def getCurrentIds(): Set[Int] = {
    return zookeeperClient.liveBrokers.toSet
  }
}
