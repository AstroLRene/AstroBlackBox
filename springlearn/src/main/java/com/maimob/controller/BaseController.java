package com.maimob.controller;

import com.maimob.base.BaseResponse;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;

public class BaseController {


    public static BaseResponse baseResponse = new BaseResponse();
    public String checkPara(HttpServletRequest request){
        String value = "";
        try{
            request.setCharacterEncoding("UTF-8");
            StringBuffer sb = new StringBuffer();
            String line = null;
            BufferedReader reader = request.getReader();
            while((line = reader.readLine()) != null){
                sb.append(line);
            }
            value = sb.toString();
        }catch (Exception e){
            e.printStackTrace();
        }
        return value;
    }
}
