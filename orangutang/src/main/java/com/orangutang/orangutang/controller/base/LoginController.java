package com.orangutang.orangutang.controller.base;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class LoginController {
    @RequestMapping("/")
    public String redirectLogin(){
        return "/index";
    }
}
