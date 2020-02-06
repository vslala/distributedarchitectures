package org.dist.simplekafka.mysimplekafka

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{Controller, ReplicaManager, SimpleKafkaApi, SimpleSocketServer}
import org.dist.util.Networks
import org.scalatest.FunSuite

class MyBrokerControllerTest extends ZookeeperTestHarness {

  test("should register for broker changes") {
    val config1 = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val config2 = new Config(2, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    
    val zookeeperClient = MyZookeeperClient(config1)
    zookeeperClient.subscribeBrokerChangeListener(MyBrokerChangeListener(zookeeperClient))
    
    val brokerController1 = MyBrokerController(zookeeperClient, config1.brokerId)
    val brokerController2 = MyBrokerController(zookeeperClient, config2.brokerId)
    
    zookeeperClient.subscribeBrokerChangeListener(MyBrokerChangeListener(zookeeperClient))
    zookeeperClient.registerSelf(brokerController1)
    zookeeperClient.registerSelf(brokerController2)
    
    brokerController1.startUp()
    
    assert(brokerController1.brokerId == config1.brokerId)
    assert(brokerController1.getLiveBrokers().size == 2)
  }

}
