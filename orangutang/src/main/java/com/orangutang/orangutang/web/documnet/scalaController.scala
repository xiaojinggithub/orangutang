package com.orangutang.orangutang.web.documnet

import org.springframework.web.bind.annotation.{RequestMapping,  RestController}

@RestController
@RequestMapping(Array("/hello"))
class scalaController {
    @RequestMapping(Array("","/"))
    def  hello():String={
      return  "a big  orangutang"
    }
}


