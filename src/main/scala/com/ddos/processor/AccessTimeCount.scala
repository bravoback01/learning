package com.ddos.processor

import scala.collection.immutable.Map

case class AccessTimeCount(key:String, dtCnt:Map[String,Int])