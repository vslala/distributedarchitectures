package org.dist.simplekafka.mysimplekafka

import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.{CreateTopicCommand, PartitionReplicas, TopicChangeHandler, ZookeeperClientImpl}
import org.dist.util.Networks

class MyTopicChangeHandlerTest extends ZookeeperTestHarness {

  class TestContext {
    var replicas: Seq[PartitionReplicas] = List()

    def leaderAndIsr(topicName: String, replicas: Seq[PartitionReplicas]) = {
      this.replicas = replicas
    }
  }

  test("should register for topic change and get replica assignments") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient = new MyZookeeperClient(config)
    
    val brokerChangeListener = new MyBrokerChangeListener(zookeeperClient)
    zookeeperClient.subscribeBrokerChangeListener(brokerChangeListener)
    
    zookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    zookeeperClient.registerBroker(Broker(1, "10.10.10.11", 8001))
    zookeeperClient.registerBroker(Broker(2, "10.10.10.12", 8002))
    
    
    val createTopicCommand = MyCreateTopicCommand(zookeeperClient)
    val testContext = new TestContext
    val topicChangeListener = new MyTopicChangeHandler(zookeeperClient, testContext.leaderAndIsr)
    zookeeperClient.subscribeTopicChangeListener(topicChangeListener)
    
    createTopicCommand.createTopic("topic1", 2, 2)

    TestUtils.waitUntilTrue(() => {
      testContext.replicas.size > 0
    }, "Waiting for topic metadata", 1000)
  }


} 
