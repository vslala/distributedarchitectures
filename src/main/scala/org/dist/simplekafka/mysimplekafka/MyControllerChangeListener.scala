package org.dist.simplekafka.mysimplekafka

import org.I0Itec.zkclient.{IZkDataListener, ZkClient}

case class MyControllerChangeListener(controller: MyBrokerController, zkClient: ZkClient) extends IZkDataListener {
  override def handleDataChange(dataPath: String, data: Any): Unit = {
    val existingControllerId: String = zkClient.readData(dataPath)
    controller.setCurrent(existingControllerId.toInt)
  }

  override def handleDataDeleted(dataPath: String): Unit = {
    controller.elect()
    if (controller.currentLeader.equals(controller.brokerId)) {
      controller.electNewLeaderForPartition();
    }
  }
}
