package com.orangutang.orangutang.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class testController {
    @RequestMapping(value = "orangutang",method = RequestMethod.GET)
    public String test(){
        scalaController scalaController=new scalaController();
        scalaController.hello();
        return "/index";
    }
}
