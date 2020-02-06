package org.dist.simplekafka.mysimplekafka

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.utils.ZkUtils
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{Controller, LeaderAndReplicas, PartitionReplicas, ZookeeperClient}
import org.scalatest.FunSuite

class MyCreateTopicCommandTest extends FunSuite {

  test("should create new topic on the broker") {
    val zookeeperClient = TestZookeeper()
    val createTopicCommand = MyCreateTopicCommand(zookeeperClient)
    val noOfPartitions = 3
    val replicationFactor = 2
    createTopicCommand.createTopic("topic1", noOfPartitions, replicationFactor)
    assert(zookeeperClient.topicName == "topic1")
    assert(zookeeperClient.partitionReplicas.size == noOfPartitions)
    zookeeperClient.partitionReplicas.map(p => p.brokerIds).foreach(_.size == replicationFactor)
  }

}

case class TestZookeeper() extends ZookeeperClient {
  
  var topicName:String = null
  var partitionReplicas = Set[PartitionReplicas]()
  var topicChangeListener: IZkChildListener = null


  override def shutdown(): Unit = ???

  override def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas]): Unit = {
    this.topicName = topicName
    this.partitionReplicas = partitionReplicas
  }

  override def setPartitionLeaderForTopic(topicName: String, leaderAndReplicas: List[LeaderAndReplicas]): Unit = {
    None
  }

  override def getAllBrokerIds(): Set[Int] = Set(0, 1, 2)

  override def getAllBrokers(): Set[ZkUtils.Broker] = Set()

  override def getPartitionReplicaLeaderInfo(topicName: String): List[LeaderAndReplicas] = ???

  override def getTopics(): List[String] = ???

  override def getBrokerInfo(brokerId: Int): ZkUtils.Broker = Broker(1, "", 0)

  override def getPartitionAssignmentsFor(topicName: String): List[PartitionReplicas] = ???

  override def subscribeTopicChangeListener(listener: IZkChildListener): Option[List[String]] = {
    this.topicChangeListener = listener
    None
  }

  override def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]] = {
    None
  }

  override def subscribeControllerChangeListner(controller: Controller): Unit = ???

  override def registerSelf(): Unit = ???

  override def tryCreatingControllerPath(data: String): Unit = ???

  override def registerSelf(controller: MyBrokerController): Unit = ???
}
