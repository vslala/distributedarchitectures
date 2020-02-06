package org.dist.simplekafka.mysimplekafka

import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{BrokerChangeListener, ControllerExistsException, LeaderAndReplicas, PartitionReplicas, SimpleSocketServer, TopicChangeHandler}

case class MyBrokerController(zookeeperClient: MyZookeeperClient, brokerId: Int) {
  var liveBrokers: Set[Int] = Set()
  var currentLeader: Int = 0
  
  def getLiveBrokers(): Set[Int] = {
    zookeeperClient.liveBrokers
  }
  
  def electNewLeaderForPartition() = ???

  def elect() = {
    val leaderId = s"${brokerId}"
    try {
      zookeeperClient.tryCreatingControllerPath(leaderId)
      this.currentLeader = brokerId;
      onBecomingLeader()
    } catch {
      case e: ControllerExistsException => {
        this.currentLeader = e.controllerId.toInt
      }
    }
  }


  def onBecomingLeader() = {
    liveBrokers = liveBrokers ++ zookeeperClient.getAllBrokerIds()
    zookeeperClient.subscribeTopicChangeListener(new MyTopicChangeHandler(zookeeperClient, onTopicChange))
    zookeeperClient.subscribeBrokerChangeListener(MyBrokerChangeListener(zookeeperClient))
  }
  
  def onTopicChange(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
    println("Topic Changed: " + topicName)
  }


  def setCurrent(existingControllerId: Int): Unit = {
    this.currentLeader = existingControllerId
  }

  def startUp(): Unit = {
    zookeeperClient.subscribeControllerChangeListener(this)
  }


}
