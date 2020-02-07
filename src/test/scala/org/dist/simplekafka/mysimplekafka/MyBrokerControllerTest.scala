package org.dist.simplekafka.mysimplekafka

import java.util

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.api.RequestOrResponse
import org.dist.queue.server.Config
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.SimpleSocketServer
import org.dist.util.Networks

class MyBrokerControllerTest extends ZookeeperTestHarness {
  val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))

  class TestSocketServer(config: Config) extends SimpleSocketServer(config.brokerId, config.hostName, config.port, null) {
    var messages = new util.ArrayList[RequestOrResponse]()
    var toAddresses = new util.ArrayList[InetAddressAndPort]()

    override def sendReceiveTcp(message: RequestOrResponse, to: InetAddressAndPort): RequestOrResponse = {
      this.messages.add(message)
      this.toAddresses.add(to)
      RequestOrResponse(message.requestId, "", message.correlationId)
    }
  }

  val testSocketServer = new TestSocketServer(config)

  test("should register for broker changes") {
    val config1 = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val config2 = new Config(2, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))

    val zookeeperClient = MyZookeeperClient(config1)
    zookeeperClient.subscribeBrokerChangeListener(MyBrokerChangeListener(zookeeperClient))

    val brokerController1 = MyBrokerController(zookeeperClient, config1.brokerId, testSocketServer)
    val brokerController2 = MyBrokerController(zookeeperClient, config2.brokerId, testSocketServer)

    zookeeperClient.subscribeBrokerChangeListener(MyBrokerChangeListener(zookeeperClient))
    zookeeperClient.registerSelf(brokerController1)
    zookeeperClient.registerSelf(brokerController2)

    brokerController1.startUp()

    assert(brokerController1.brokerId == config1.brokerId)
    assert(brokerController1.getLiveBrokers().size == 2)
  }

  test("should elect first server as controller") {
    val config1 = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient = MyZookeeperClient(config1)
    val controller = MyBrokerController(zookeeperClient, config1.brokerId, testSocketServer)
    zookeeperClient.subscribeBrokerChangeListener(MyBrokerChangeListener(zookeeperClient))
    zookeeperClient.registerSelf(controller)

    controller.startUp()

    assert(controller.currentLeader === config1.brokerId)
  }

  test("should elect first server as controller and register for topic change") {
    val config1 = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient = MyZookeeperClient(config1)
    val controller = MyBrokerController(zookeeperClient, config1.brokerId, testSocketServer)
    zookeeperClient.registerSelf(controller)
    
    controller.startUp() // first controller/broker will startup
    
    MyCreateTopicCommand(zookeeperClient).createTopic("New Topic", 1, 1)


    TestUtils.waitUntilTrue(() => {
      testSocketServer.messages.size() == 2
    }, "Waiting for message to be received", 1000)
    assert(controller.currentLeader === config1.brokerId)
  }

}
