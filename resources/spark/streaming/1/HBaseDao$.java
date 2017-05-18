/**
  * HBase 数据库操作
  *
  * Created by TangXD on 2017/4/20.
  */
object HBaseDao extends Serializable {

    @transient lazy val logger = LogManager.getLogger(HBaseDao.getClass)

    private val connection: Connection = createHBaseConn


    def createHBaseConn: Connection = {
        val conf: Configuration = HBaseConfiguration.create()
        conf.addResource(this.getClass().getResource("/hbase-site.xml"))
        ConnectionFactory.createConnection(conf)
    }



    /**
      * 获取表
      *
      * @param tableName
      * @return
      */
    def getTable(tableName: String): HTable = {
        val table: Table = connection.getTable(TableName.valueOf(tableName))
        table.asInstanceOf[HTable]
    }

    /**
      *
      * 插入数据到 HBase
      *
      * 参数( tableName ,  json ) )：
      *
      * Json格式：
      *     {
      *         "rowKey": "00000-0",
      *         "family:qualifier": "value",
      *         "family:qualifier": "value",
      *         ......
      *     }
      *
      * @param data
      * @return
      */
    def insert(data: (String, String)): Boolean = {

        val t: HTable = getTable(data._1) //HTable
        try {
            val map: mutable.HashMap[String, Object] = JsonUtils.json2Map(data._2)
            val rowKey: Array[Byte] = String.valueOf(map.get("rowKey")).getBytes //rowKey
            val put = new Put(rowKey)

            for ((k, v) <- map) {
                val keys: Array[String] = k.split(":")
                if (keys.length == 2){
                    put.addColumn(keys(0).getBytes, keys(1).getBytes, String.valueOf(v).getBytes)
                }
            }

            Try(t.put(put)).getOrElse(t.close())
            true
        } catch {
            case e: Exception =>
                e.printStackTrace()
                false
        }
    }
}
