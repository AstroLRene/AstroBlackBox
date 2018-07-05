package com.astro.utils

import java.text.SimpleDateFormat
import java.util.Date

object DataUtil {

  def transformat(time:String,pattern:String):String = {
    val format = new SimpleDateFormat(pattern)
    format.format(new Date(time.toLong))
  }

// 判断字符是否为包含表情符号
  def isEmojiCharacter(c:Char): Boolean = {
    return (c == 0x0) ||
            (c == 0x9) ||
            (c == 0xA) ||
            (c == 0xD) ||
            ((c >=0x20) && (c <= 0xD7FF)) ||
            ((c >= 0xE000) && (c <= 0xFFFD)) ||
            ((c >= 0x10000) && (c <= 0x10FFFF))
  }
// 过滤表情符号
  def filterEmoji(str:String):String = {
    val len = str.length
    var buf = new StringBuffer()
    for(i <- 0 until(len)){
      val c = str.charAt(i)
      if(isEmojiCharacter(c)){
        buf.append(c)
      }
    }
    return buf.toString
  }
}
