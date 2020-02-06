package org.dist.simplekafka.mysimplekafka

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.util.Networks

class MyBrokerChangeListenerTest extends ZookeeperTestHarness {

  test("it should subscribe to the registered broker and invoke a callback method") {
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val broker1 = Broker(config1.brokerId, config1.hostName, config1.port)

    val config2 = Config(2, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val broker2 = Broker(config2.brokerId, config2.hostName, config2.port)

    val zookeeperClient: MyZookeeperClient = MyZookeeperClient(config1)
    val listener = MyBrokerChangeListener(zookeeperClient)
    zookeeperClient.subscribeBrokerChangeListener(listener)
    zookeeperClient.registerBroker(broker1)
    zookeeperClient.registerBroker(broker2)

    TestUtils.waitUntilTrue(() => {
      listener.getCurrentIds().size == 2
    }, "Waiting for zookeeper clients to register", 1000)
    
    assert(listener.getCurrentIds().size === 2)
  }
}
