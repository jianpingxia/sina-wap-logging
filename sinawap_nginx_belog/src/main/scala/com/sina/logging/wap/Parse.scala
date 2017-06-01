package com.sina.logging.wap

import java.text.SimpleDateFormat
import java.util.Locale

/**
  * Created by jianping on 2017/6/1.
  */
object Parse {
  def main(args: Array[String]): Unit = {
    //'[$time_local]`$http_x_up_calling_line_id`"$request"`"$http_user_agent"`$status`[$remote_origin_addr]`-`"$http_referer"`$request_time`$body_bytes_sent`$upstream_cache_status`$http_x_forwarded_for`$host`$http_cookie'
    val s = "{| [01/Jun/2017:15:09:00 +0800]`-`\"GET /intercept_d.html/?chname=photo&times=1 HTTP/1.1\"`\"Mozilla/5.0 (Linux; U; Android 6.0.1;zh_cn; Le X820 Build/FEXCNFN5902605092S) AppleWebKit/537.36 (KHTML, like Gecko)Version/4.0 Chrome/49.0.0.0 Mobile Safari/537.36 EUI Browser/1.1.1.18\"`302`[14.24.30.241]`-`\"-\"`0.001`5`MISS`14.24.30.241, 172.16.92.28`photo.sina.cn`ustat=__183.21.246.25_1494260903_0.42103100; genTime=1494260903; Apache=7379188943973.669.1494260904270; ULV=1494260904274:1:1:1:7379188943973.669.1494260904270:; SINAGLOBAL=7379188943973.669.1494260904270; timeNumber=4981935; timestamp=4981935Rbxr0V; sinaWapAddToHome_SetTime=2017-05-18; sinaWapAddToHome_SetTime_dfz_guangdong=2017-05-23; ANTI_ADB_HOST=sax.sina.cn; ANTI_ADB_KEYS=rotate_count%2Ccallback%2Cadunit_id%2Ctimestamp%2Cpage_url%2Cnpic; sinaPhotoShareTips=1; dfz_loc=gd-default; wm=3273; SMART=1; vt=4; historyRecord={\\\\x22href\\\\x22:\\\\x22https://photo.sina.cn/album_1_86523_150918.htm\\\\x22,\\\\x22refer\\\\x22:\\\\x22\\\\x22}|10.83.0.89|api003.dpool.msina.sx.sinanode.com|}"
    val parts1 = s.split("`")
    val parts2 = s.split("\\|")
    val timestamptmp = parts1(0).split("\\[")
    for (x <- timestamptmp){
      println(x)
    }
    //val timestamp = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH).parse(timestamptmp(2).substring(0, timestamptmp(2).indexOf("]")))
    //println(timestamp)
    println("=========================")

  }
}
