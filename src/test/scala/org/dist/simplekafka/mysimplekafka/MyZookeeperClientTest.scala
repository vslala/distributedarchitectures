package org.dist.simplekafka.mysimplekafka

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.util.Networks

class MyZookeeperClientTest extends ZookeeperTestHarness {
  
  test("it should register broker to the zookeeper and create ephemeral path for it") {
    val config = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: MyZookeeperClient = MyZookeeperClient(config)
    val broker = Broker(config.brokerId, config.hostName, config.port)
    zookeeperClient.registerBroker(broker)
    
    assert(zookeeperClient !== null)
    assert(zookeeperClient.getAllBrokers().contains(broker))
  }
  
  
}
