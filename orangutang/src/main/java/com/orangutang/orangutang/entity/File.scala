package com.orangutang.orangutang.entity
//主构造器
class File(var size:Long,var fileName:String) {
  print("直接在类中不在方法中可以写输出语句")
  var path:String=_
  //附属构造器
  def this(size:Long,fileName:String, path:String){
     this(size,fileName)   //附属构造器的第一行必须要调用主构造器或者是其他的构造器
     this.path=path   //构造器给属性赋值
  }

}
