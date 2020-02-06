package org.dist.simplekafka

import java.util

import akka.actor.ActorSystem
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config

case class ReplicaManager(config:Config)(implicit actorSystem:ActorSystem) {
  val allPartitions = new util.HashMap[TopicAndPartition, Partition]()

  def makeFollower(topicAndPartition: TopicAndPartition, leaderId:Int) = {
    val partition = getOrCreatePartition(topicAndPartition)
    partition.makeFollower(leaderId)
  }

  def makeLeader(topicAndPartition: TopicAndPartition) = {
    val partition = getOrCreatePartition(topicAndPartition)
    partition.makeLeader()
  }

  def getPartition(topicAndPartition: TopicAndPartition) = {
    allPartitions.get(topicAndPartition)
  }

  def getOrCreatePartition(topicAndPartition: TopicAndPartition) = {
    var partition = allPartitions.get(topicAndPartition)
    if (null == partition) {
      partition = new Partition(config, topicAndPartition)
      allPartitions.put(topicAndPartition, partition)
    }
    partition
  }

}
