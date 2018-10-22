package com.orangutang.orangutang.controller

import com.orangutang.orangutang.entity.User
import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, RestController}

import scala.collection.mutable.ListBuffer

@RestController
@RequestMapping(Array("/hello"))
class scalaController {
  @RequestMapping(value = Array("/greeting"), method = Array(RequestMethod.GET))
  def  hello():String={
      "a big  orangutang"
  }



  def  test(x:Int,y: Int):Int= {
      print
      x+y
  }

  def print(): Unit = {
    println("test hello")
  }

  def getReturn(x:Int,y:Int)=x+y

  def test4(str:String="hello"): Unit ={
    println(str)
  }
  def test5(): Unit ={
    println( test4("hello word"))
  }


  def test6(num1:Long,num2:Long) ={
    num1+num2
  }

  def test7(): Long ={
    test6(num2=100,num1=100)
  }
  def test8(): Unit ={
    var a=0
    var b=if (a>0)
    true
    else
    false
  }

  @RequestMapping(value = Array("/fori"), method = Array(RequestMethod.GET))
  def fori(value:String,name:String){
//    println(1 to 10)
//    println(1.to(10))

//    var l=List(1,2,3,4)
//    l.head
//    l.tail
//    var l2=1::Nil
//    var l3=3::l
//
//    var l4=ListBuffer[Int]()
//    l4+=5
//    l4++=l2

     value match {
       case "a"=>println("a")
       case "b"=>println("b")
       case "c" if(name=="xj")=>println("c")
       case _ =>println("d")
     }
  }

  def arraymatch(array:Array[String]): Unit = {
      array match{
        case Array("name1")=>println("name1")
        case Array("name2","name3")=>println("name2 and name3")
        case Array("name2",_*)=>println("name2 and more")
        case _=>println("no include name")
      }
  }
  def typeMatch(x:Any): Unit ={
    x match {
      case x:Int=>println("Int")
      case x:String=>println("String")
      case x:Map[_,_]=>{println("Map")}
    }
  }
  @RequestMapping(value = Array("/mapMatch"), method = Array(RequestMethod.GET))
  def  mapMatch(): Unit ={
     typeMatch(Map("key"->"value"))
  }

  @RequestMapping(value = Array("/arraymatch"), method = Array(RequestMethod.GET))
  def  testArrayMatch(): Unit ={
    arraymatch(Array("name1"))
    arraymatch(Array("name2","name3"))
  }


  @RequestMapping(value = Array("/trycatch"), method = Array(RequestMethod.GET))
  def  trycatch(): Unit ={
    try {
       println(10/0)
    }catch {
      case e:ArithmeticException=>{println("ArithmeticException")}
      case e:Exception=>{println("other Exception")}
    }
  }

  class  Person
  case class student(var name:String)  extends  Person
  case class workera(var name:String)  extends  Person

  def  testCaseClass(person: Person): Unit ={
     person match {
       case student("xj")=>println("student")
       case workera("x")=>{println("worker")}
       case _=>println("other")
     }
  }

  @RequestMapping(value = Array("/testCaseClass"), method = Array(RequestMethod.GET))
  def  testCaseClasstt(): Unit ={
    testCaseClass(student("xjj"))
  }

  @RequestMapping(value = Array("/strtest"), method = Array(RequestMethod.GET))
  def testStr(): Unit ={
    var str1="hello"
    var str2="word"
    println(s"$str1 $str2")

    var str3=
      """
        |hello
        |word
        |test
        |xxxxx
      """.stripMargin
    println(str3+s"$str1")
  }

  (str:String)=>println(str)


  def sum1(a:Int,b:Int):Int=a+b
  def sum2(a:Int)(b:Int):Int=a+b
  def carrying(): Unit ={
    sum1(1,2)
    sum2(1)(2)
  }

  @RequestMapping(value = Array("/testfunc"), method = Array(RequestMethod.GET))
  def testfunc(): Unit ={
    var l=List(1,2,3.4,5,6)
   /* l.map((x)=>x*2)
    for (n<-l) println(n)
    l.map(x=>x*2)
    for (n<-l) println(n)
    l.map(_*2)
    for (n<-l) println(n)*/

    println(l.reduce(_+_))
  }

}


