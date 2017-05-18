package com.xiaoxiaomo.utils.common

import kafka.utils.{immutable => _}
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer

/**
  * Created by TangXD on 2017/4/25.
  */
object ZKStringSerializer extends ZkSerializer {

    @throws(classOf[ZkMarshallingError])
    def serialize(data: Object): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

    @throws(classOf[ZkMarshallingError])
    def deserialize(bytes: Array[Byte]): Object = {
        if (bytes == null)
            null
        else
            new String(bytes, "UTF-8")
    }
}

