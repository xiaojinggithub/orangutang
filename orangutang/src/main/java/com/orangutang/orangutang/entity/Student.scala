package com.orangutang.orangutang.entity

import scala.collection.mutable.ArrayBuffer


class User1(var age:Int,var name:String){
    var address:String=_
    def this( age:Int, name:String, address:String){
      this( age:Int, name:String)
    }
    //父类方法
    def eat(): Unit ={
      println("father eat")
    }
}

class Student(age:Int,name:String,var hobby:String)extends User1(age:Int,name:String){
    def this(age:Int,name:String,hobby:String,email:String){
      this(age:Int,name:String,hobby:String)
    }

  //子类继承父类的方法
  override def eat(): Unit = println("children eat")
}

//class student1(age:Int,name:String,var hobby:String)extends User1( age:Int, name:String,address:String){
//
//}
class test(){
  var student=new Student(33,"xx","xxxx");
}

abstract class  User2{
  def eat
  var username:String
}

class studenet extends User2{
  override def eat: Unit = println("xxx")
  override var username:String="xx"
}

//
class Apply{
  def apply(): Unit ={
    print("class apply")
  }
}
object Apply{
  println("object...")

  def apply(): Unit ={
    print("object apply")
  }
}

class use(){
  def test(): Unit ={
    Apply.apply()
    var a=new Apply
    a.apply()
  }
}

case class testCase(name:String)
class useCase(){
  testCase("xxx").name
}

class  arrayTest(){
  var a=new Array[String](5)
  a(1)="xxxx"
  a(1)

  var b=Array("xxx","xcc")
  var x=ArrayBuffer[Int]()
  x+=(1,2,3)
  x.insert(2,1)


  var l=List(1,2,3,4)
  l.head
  l.tail
  var l2=1::Nil

  //Option(str:String)
  val ee:Option[Int] = Some(5)
  val ww:Option[Int] = None
  println("a.getOrElse(0): " + ee.getOrElse(0) )
  println("b.getOrElse(10): " + ww.getOrElse(10) )

  var tupleTest=(1,"2",3,4.0)
//  for(i<-tupleTest){
//    println(i)
//  }
}