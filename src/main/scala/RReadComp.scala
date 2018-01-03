import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * Created by lxb on 1/2/2018.
  */
object RReadComp {

  val tableName_high = "airline_search_high"
  val tableName_broad = "airline_search_broad"

  val sdf = new SimpleDateFormat("MMdd");
  val rkeys:Array[String] =  Array("fdaDYGZUH1204","fdaDYGZUH1224","fdaLYGJUZ0707","fdaLYGJUZ0817","fdaLYGJUZ0927","fdaLYGJUZ1017","fdaLYGJUZ1106","fdaLYGJUZ1127","fdaLYGJUZ1216","05eJNGTAO1012","05eJNGTAO1101","05eJNGTAO1122","05eJNGTAO1212","062YINDNH0103","062YINDNH0728","062YINDNH0908","062YINDNH1007","062YINDNH1028","062YINDNH1117","062YINDNH1208","062YINDNH1228","064HSNHGH0717","064HSNHGH0827","064HSNHGH1002","064HSNHGH1022","064HSNHGH1112","fdeACXHRB1201","fdeACXHRB1220","fe7KHNWUX0630","fe7KHNWUX0810","792MIGJZH0813","792MIGJZH0925","792MIGJZH1015","792MIGJZH1104","792MIGJZH1124","792MIGJZH1214","797TVSYNT0103","797TVSYNT0727","797TVSYNT0907","797TVSYNT1007","797TVSYNT1027","797TVSYNT1116","797TVSYNT1207","797TVSYNT1227","798TNADNH0713","798TNADNH0822","798TNADNH0929","798TNADNH1020","798TNADNH1109","798TNADNH1129","798TNADNH1219","79dHDGHFE0628","79dHDGHFE0807","79dHDGHFE0919","79dHDGHFE1013","79dHDGHFE1102","79dHDGHFE1122","79dHDGHFE1211","79dHDGHFE1231","79eLZOXIY0721","79eLZOXIY0831","79eLZOXIY1224","7a7NAYTAO0706","7a7NAYTAO0816","7a7NAYTAO0926","fe8NAYHGH1026","fe8NAYHGH1115","fe8NAYHGH1205","fe8NAYHGH1225","feaKOWTYN0708","feaKOWTYN0820","feaKOWTYN0927","feaKOWTYN1018","feaKOWTYN1107","feaKOWTYN1128","feaKOWTYN1217","8a1TSNSHP1111","8a1TSNSHP1202","8a1TSNSHP1223","8a2CKGSHP0705","8a2CKGSHP0815","8a2CKGSHP0926","8a2CKGSHP1016","8a2CKGSHP1105","8a2CKGSHP1125","8a2CKGSHP1215","8b5DDGDLC0619","8b5DDGDLC0729","8b5DDGDLC0911","8b5DDGDLC1008","8b5DDGDLC1029","8b5DDGDLC1117","8b5DDGDLC1208","8b5DDGDLC1228","8baHDGTYN0716","8baHDGTYN0824","8baHDGTYN0930","8baHDGTYN1021","8baHDGTYN1110","8baHDGTYN1130","8baHDGTYN1220","8bcTYNNAY0701")
  //val rkeys:Array[String] = Array("2ddNAOSHA1130","2ddNAOSHA1220","2e0SHPTVS0701","2e0SHPTVS0809","2e0SHPTVS0921","2e0SHPTVS1014","2e0SHPTVS1104","2e0SHPTVS1123","2e0SHPTVS1213","2e5YBPHET0102","2e5YBPHET0723","2e5YBPHET0903","2e5YBPHET1005","2e5YBPHET1025","2e5YBPHET1115","2e5YBPHET1205","2e5YBPHET1225","2edCKGCIF0711","2edCKGCIF0820","2edCKGCIF0928","2edCKGCIF1019")
  val rkeys_broad:Array[String] = Array("fdbTLQDNH0620","fdbTLQDNH0801","fdbTLQDNH0911","fdbTLQDNH1008","fdbTLQDNH1029","fdbTLQDNH1118","fdbTLQDNH1208","fdbTLQDNH1229","fdeACXHRB0717","fdeACXHRB0827","fdeACXHRB1001","fdeACXHRB1022","fdeACXHRB1111","046SZXTLQ1103","046SZXTLQ1123","046SZXTLQ1215","057NLTYIN0619","057NLTYIN0803","057NLTYIN0916","057NLTYIN1012","057NLTYIN1101","057NLTYIN1121","057NLTYIN1212","057WEHTYN0101","057WEHTYN0725","057WEHTYN0906","057WEHTYN1007","057WEHTYN1028","057WEHTYN1117","057WEHTYN1208","fe7KHNWUX1212","fe8NAYHGH0101","fe8NAYHGH0725","fe8NAYHGH0904","fe8NAYHGH1006","785LYAWEH0728","785LYAWEH0907","785LYAWEH1007","785LYAWEH1026","785LYAWEH1116","785LYAWEH1206","785LYAWEH1226","787LHWBHY0712","787LHWBHY0822","787LHWBHY0929","787LHWBHY1020","787LHWBHY1109","787LHWBHY1129","787LHWBHY1219","78dLYATSN0628","78dLYATSN0809","78dLYATSN0917","78dLYATSN1012","78dLYATSN1031","78dLYATSN1121","78dLYATSN1210","78dLYATSN1231","78eSHAHSN0724","78eSHAHSN0902","78eSHAHSN1005","8bcTYNNAY1014","8bcTYNNAY1104","8bcTYNNAY1123","8bcTYNNAY1214","8bdCIFTYN0103","8bdCIFTYN0726","8bdCIFTYN0906","8bdCIFTYN1008","8bdCIFTYN1027","8bdCIFTYN1117","8bdCIFTYN1206","8bdCIFTYN1226","8ccCGQHLH0714","8ccCGQHLH0826","8ccCGQHLH1001","8ccCGQHLH1020","8ccCGQHLH1109","8ccCGQHLH1129","8ccCGQHLH1218","8cdJNGBAV0623","8cdJNGBAV0803","8cdJNGBAV0914","febCGQNNY0623","febCGQNNY0803","febCGQNNY0912","febCGQNNY1008","febCGQNNY1029","febCGQNNY1118","febCGQNNY1209","febCGQNNY1230","febWUSYNT0721","febWUSYNT0901","febWUSYNT1004","febWUSYNT1024","febWUSYNT1113","febWUSYNT1203","febWUSYNT1222","fecBAVNNY0703","fecBAVNNY0812","fecBAVNNY0923","fecBAVNNY1014")

  def main(args: Array[String]): Unit = {
    val conf:Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "172.29.100.21,172.29.100.22,172.29.100.23")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)

    var cal: Calendar = Calendar.getInstance()
    val MD = sdf.format(cal.getTime)
    cal.add(Calendar.DAY_OF_YEAR, 1)
    val MD2 = sdf.format(cal.getTime)

    /*//Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)*/

    val userTable_high: TableName = TableName.valueOf(tableName_high)

    //获取 user 表
    val table = conn.getTable(userTable_high)

    val keysize = rkeys_broad.size
    var i = 0
    val tb_broad = conn.getTable(TableName.valueOf(tableName_broad))
    try {
      val broad_start_sys  = System.currentTimeMillis()
      while(i<keysize) {
        val g = new Get(rkeys_broad(i).getBytes)
        g.setMaxVersions()
        val prerev = tb_broad.get(g)
        if(prerev.isEmpty){
          println(rkeys_broad(i))
        }else{
          var mmp = prerev.getFamilyMap(Bytes.toBytes("basic"))
          val ff= mmp.entrySet().iterator()
          while(ff.hasNext()){
            val kvp = ff.next()
            kvp.getKey
            kvp.getValue
            //println(Bytes.toString(kvp.getKey) + ":" + Bytes.toString(kvp.getValue))
          }
        }
        i=i+1
      }
      val broad_end_sys = System.currentTimeMillis()
      val broad_cost = (broad_end_sys-broad_start_sys)/keysize ;
      println("broad avg cost :" +broad_cost)
    } finally {
      tb_broad.close()
    }

    try {
      val keyhigh = rkeys.size
      i=0
      val high_start_sys = System.currentTimeMillis()
      while(i<keyhigh) {
        //扫描数据
        val s = new Scan()
        s.setBatch(365)
        s.setMaxVersions()
        s.setStartRow(Bytes.toBytes(rkeys(i) + "000"))
        s.setStopRow(Bytes.toBytes(rkeys(i) + "365"))
        val scanner = table.getScanner(s)
        try {
          val ite: util.Iterator[Result] = scanner.iterator();
          var arr_data: Seq[(String, String, String, String, Int)] = Seq[(String, String, String, String, Int)]()
          while (ite.hasNext) {
            var r = ite.next()
            r.getValue(Bytes.toBytes("basic"), Bytes.toBytes("linedata"))
          }

        } catch {
          case e: Exception => e.printStackTrace()
        }
        finally {
          //确保scanner关闭
          scanner.close()
        }
        i=i+1
      }
      val high_end_sys = System.currentTimeMillis()
      val high_cost = (high_end_sys-high_start_sys)/keyhigh ;
      println("high avg cost :" +high_cost)
    } finally {
        table.close()
        conn.close()
    }

  }
}
