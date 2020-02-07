package org.dist.simplekafka.mysimplekafka

import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.annotations.VisibleForTesting
import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka._

import scala.jdk.CollectionConverters._

case class MyBrokerController(zookeeperClient: MyZookeeperClient, brokerId: Int, socketServer: SimpleSocketServer) {
  val correlationId = new AtomicInteger(0)
  var liveBrokers: Set[Broker] = Set()
  var currentLeader: Int = 0
  
  def getLiveBrokers(): Set[Broker] = {
    liveBrokers
  }
  
  def electNewLeaderForPartition() = ???

  def elect() = {
    val leaderId = s"$brokerId"
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
    liveBrokers = liveBrokers ++ zookeeperClient.getAllBrokers()
    zookeeperClient.subscribeTopicChangeListener(new MyTopicChangeHandler(zookeeperClient, onTopicChange))
    zookeeperClient.subscribeBrokerChangeListener(MyBrokerChangeListener(zookeeperClient))
  }

  @VisibleForTesting
  def sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas: Seq[LeaderAndReplicas], partitionReplicas: Seq[PartitionReplicas]) = {
    val brokerToLeaderIsrRequest = new util.HashMap[Broker, java.util.List[LeaderAndReplicas]]()
    leaderAndReplicas.foreach(lr ⇒ {
      lr.partitionStateInfo.allReplicas.foreach(broker ⇒ {
        var leaderReplicas = brokerToLeaderIsrRequest.get(broker)
        if (leaderReplicas == null) {
          leaderReplicas = new util.ArrayList[LeaderAndReplicas]()
          brokerToLeaderIsrRequest.put(broker, leaderReplicas)
        }
        leaderReplicas.add(lr)
      })
    })

    val brokers = brokerToLeaderIsrRequest.keySet().asScala
    for (broker ← brokers) {
      val leaderAndReplicas: java.util.List[LeaderAndReplicas] = brokerToLeaderIsrRequest.get(broker)
      val leaderAndReplicaRequest = LeaderAndReplicaRequest(leaderAndReplicas.asScala.toList)
      val request = RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicaRequest), correlationId.getAndIncrement())
      socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
    }
  }

  def sendUpdateMetadataRequestToAllLiveBrokers(leaderAndReplicas: Seq[LeaderAndReplicas]) = {
    println("Live Brokers: " + liveBrokers.size)
    val brokerListToIsrRequestMap =
      liveBrokers.foreach(broker ⇒ {
        val updateMetadataRequest = UpdateMetadataRequest(liveBrokers.toList, leaderAndReplicas.toList)
        val request = RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest), correlationId.incrementAndGet())
        socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
      })
  }

  def onTopicChange(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
    val leaderAndReplicas: Seq[LeaderAndReplicas] = selectLeaderAndFollowerBrokersForPartitions(topicName, partitionReplicas)

    zookeeperClient.setPartitionLeaderForTopic(topicName, leaderAndReplicas.toList);
    //This is persisted in zookeeper for failover.. we are just keeping it in memory for now.
    sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas, partitionReplicas)
    sendUpdateMetadataRequestToAllLiveBrokers(leaderAndReplicas)
  }
  
  private def selectLeaderAndFollowerBrokersForPartitions(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
    val leaderAndReplicas: Seq[LeaderAndReplicas] = partitionReplicas.map(p => {
      val leaderBrokerId = p.brokerIds.head //This is where leader for particular partition is selected
      val leaderBroker = getBroker(leaderBrokerId)
      val replicaBrokers = p.brokerIds.map(id ⇒ getBroker(id))
      LeaderAndReplicas(TopicAndPartition(topicName, p.partitionId), PartitionInfo(leaderBroker, replicaBrokers))
    })
    leaderAndReplicas
  }
  
  private def getBroker(brokerId: Int) = {
    liveBrokers.find(b ⇒ b.id == brokerId).get
  }


  def setCurrent(existingControllerId: Int): Unit = {
    this.currentLeader = existingControllerId
  }

  def startUp(): Unit = {
    zookeeperClient.subscribeControllerChangeListener(this)
    elect()
  }


}
