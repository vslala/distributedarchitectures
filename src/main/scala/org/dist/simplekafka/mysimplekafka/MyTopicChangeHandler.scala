package org.dist.simplekafka.mysimplekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.simplekafka.PartitionReplicas

import scala.jdk.CollectionConverters._

class MyTopicChangeHandler(zookeeperClient: MyZookeeperClient, onTopicChange: (String, Seq[PartitionReplicas]) => Unit) extends IZkChildListener {
  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    currentChilds.asScala.foreach(topicName => {
      val replicas: Seq[PartitionReplicas] = zookeeperClient.getPartitionAssignmentsFor(topicName)

      println("Topic: " + topicName)
      onTopicChange(topicName, replicas)
    })
  }
}
