/**
  * spark consumer 消费kafka数据 存入到HBase
  *
  * 运行示例，具体见配置文件：
  * /opt/cloudera/parcels/CDH/bin/spark-submit --master yarn-client --class com.creditease.streaming.KafkaDataStream hspark-1.0.jar 1 3 1000
  *
  * 参数：
  * 每次处理条数：timeWindow * maxRatePerPartition * partitionNum
  *
  * Created by TangXD on 2017/4/24.
  */
object KafkaDataStream {

    private val logger: Logger = LoggerFactory.getLogger(KafkaDataStream.getClass)

    def main(args: Array[String]) {

        //接收参数
        val Array(kafka_topic, timeWindow, maxRatePerPartition) = args

        //加载配置
        val prop: Properties = new Properties()
        prop.load(this.getClass().getResourceAsStream("/kafka.properties"))

        val groupName = prop.getProperty("group.id");

        //获取配置文件中的topic
        val kafkaTopics: String = prop.getProperty("kafka.topic." + kafka_topic)
        if( kafkaTopics == null || kafkaTopics.length <= 0){
            System.err.println("Usage: KafkaDataStream <kafka_topic> is number from kafka.properties")
            System.exit(1)
        }

        val topics: Set[String] = kafkaTopics.split(",").toSet

        val kafkaParams = scala.collection.immutable.Map[String, String](
            "metadata.broker.list" -> prop.getProperty("bootstrap.servers"),
            "group.id" -> groupName,
            "auto.offset.reset" -> "largest")

        val kc = new KafkaCluster(kafkaParams)

        //初始化配置
        val sparkConf = new SparkConf()
                .setAppName(KafkaDataStream.getClass.getSimpleName + topics.toString())
                .set("spark.yarn.am.memory", prop.getProperty("am.memory"))
                .set("spark.yarn.am.memoryOverhead", prop.getProperty("am.memoryOverhead"))
                .set("spark.yarn.executor.memoryOverhead", prop.getProperty("executor.memoryOverhead"))
                .set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition) //此处为每秒每个partition的条数
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.reducer.maxSizeInFlight", "1m")

        val sc = new SparkContext(sparkConf)
        val ssc = new StreamingContext(sc, Seconds(timeWindow.toInt)) //多少秒处理一次请求

        //zk
        val zkClient = new ZkClient(prop.getProperty("zk.connect"), Integer.MAX_VALUE, 100000, ZKStringSerializer)
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())


        var fromOffsets: Map[TopicAndPartition, Long] = Map() //多个partition的offset


        //支持多个topic : Set[String]
        topics.foreach(topicName => {

            //去brokers中获取partition数量，注意：新增partition后需要重启
            val children = zkClient.countChildren(ZkUtils.getTopicPartitionsPath(topicName))
            for (i <- 0 until children) {

                //kafka consumer 中是否有该partition的消费记录，如果没有设置为0
                val tp = TopicAndPartition(topicName, i)
                val path: String = s"${new ZKGroupTopicDirs(groupName, topicName).consumerOffsetDir}/$i"
                if (zkClient.exists(path)) {
                    fromOffsets += (tp -> zkClient.readData[String](path).toLong)
                } else{
                    fromOffsets += (tp -> 0)
                }
            }
        })

        logger.info(s"+++++++++++++++++++ fromOffsets $fromOffsets +++++++++++++++++++++++++ ")
        //创建Kafka持续读取流，通过zk中记录的offset
        val messages: InputDStream[(String, String)] =
            KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

        //数据操作
        messages.foreachRDD(rdd => {
            val offsetsList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            //data 处理
            rdd.foreachPartition(partitionRecords => {
                //TaskContext 上下文
                val offsetRange: OffsetRange = offsetsList(TaskContext.get.partitionId)
                logger.info(s"${offsetRange.topic} ${offsetRange.partition} ${offsetRange.fromOffset} ${offsetRange.untilOffset}")

                //TopicAndPartition 主构造参数第一个是topic，第二个是Kafka partition id
                val topicAndPartition = TopicAndPartition(offsetRange.topic, offsetRange.partition)
                val either = kc.setConsumerOffsets(groupName, Map((topicAndPartition, offsetRange.untilOffset))) //是
                if (either.isLeft) {
                    logger.info(s"Error updating the offset to Kafka cluster: ${either.left.get}")
                }

                partitionRecords.foreach(data => {
                    HBaseDao.insert(data)
                })
            })

        })

        ssc.start()
        ssc.awaitTermination()
    }

}
