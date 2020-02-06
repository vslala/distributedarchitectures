package org.dist.simplekafka.mysimplekafka

import org.dist.simplekafka.{PartitionReplicas, ZookeeperClient}

import scala.collection.mutable
import scala.util.Random

case class MyCreateTopicCommand(zookeeperClient: ZookeeperClient) {
  val rand = Random

  private def getWrappedIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }

  def assignReplicasToBrokers(brokerList: List[Int], noOfPartitions: Int, replicationFactor: Int): Set[PartitionReplicas] = {
    
    val ret = new mutable.HashMap[Int, List[Int]]()
    val startIndex = rand.nextInt(brokerList.size)
    var currentPartitionId = 0

    var nextReplicaShift = rand.nextInt(brokerList.size)
    for (partitionId <- 0 until noOfPartitions) {
      if (currentPartitionId > 0 && (currentPartitionId % brokerList.size == 0))
        nextReplicaShift += 1
      val firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size
      var replicaList = List(brokerList(firstReplicaIndex))
      for (replicaIndex <- 0 until replicationFactor - 1)
        replicaList ::= brokerList(getWrappedIndex(firstReplicaIndex, nextReplicaShift, replicaIndex, brokerList.size))
      ret.put(currentPartitionId, replicaList.reverse)
      currentPartitionId = currentPartitionId + 1
    }
    val partitionIds = ret.toMap.keySet
    partitionIds.map(id => PartitionReplicas(id, ret(id)))
  }
  

  def createTopic(topicName: "topic1", noOfPartitions: Int, replicationFactor: Int) = {
    val brokerIds = zookeeperClient.getAllBrokerIds()
    
    val partitionReplicas: Set[PartitionReplicas] = assignReplicasToBrokers(brokerIds.toList, noOfPartitions, replicationFactor)
    
    zookeeperClient.setPartitionReplicasForTopic(topicName, partitionReplicas)
  }

}
