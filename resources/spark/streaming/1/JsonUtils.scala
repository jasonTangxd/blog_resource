package com.xiaoxiaomo.utils

import java.util

import net.minidev.json.{JSONObject}
import net.minidev.json.parser.JSONParser
import scala.collection.mutable
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.mutableMapAsJavaMap


/**
  * json-smart
  * json 解析
  *
  * <dependency>
  *     <groupId>net.minidev</groupId>
  *     <artifactId>json-smart</artifactId>
  *     <version>1.3.1</version>
  * </dependency>
  * Created by TangXD on 2017/4/25.
  */
object JsonUtils extends App{

    /**
      * 将map转为json
      * @param map 输入格式 mutable.Map[String,Object]
      * @return
      * */
    def map2Json(map : mutable.Map[String,Object]) : String = {
        val jsonString = JSONObject.toJSONString(map)
        jsonString
    }


    /**
      * 将json转化为Map
      * @param json 输入json字符串
      * @return
      * */
    def json2Map(json : String) : mutable.HashMap[String,Object] = {

        val map : mutable.HashMap[String,Object]= mutable.HashMap()
        val jsonParser =new JSONParser(JSONParser.MODE_PERMISSIVE)

        //将string转化为jsonObject
        val jsonObj: JSONObject = jsonParser.parse(json).asInstanceOf[JSONObject]

        //获取所有键
        val jsonKey = jsonObj.keySet()
        val iter = jsonKey.iterator()

        while (iter.hasNext){
            val field = iter.next()
            val value = jsonObj.get(field).toString
            if(value.startsWith("{")&&value.endsWith("}")){
                val value = mapAsScalaMap(jsonObj.get(field).asInstanceOf[util.HashMap[String, String]])
                map.put(field,value)
            }else{
                map.put(field,value)
            }
        }
        map
    }
}