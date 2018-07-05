package com.maimob.base;

import com.alibaba.fastjson.JSONObject;

public class BaseResponse {

    private String returnCode;

    private String returnMsg;

    private Object response;

    public String getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(String returnCode) {
        this.returnCode = returnCode;
    }

    public String getReturnMsg() {
        return returnMsg;
    }

    public void setReturnMsg(String returnMsg) {
        this.returnMsg = returnMsg;
    }

    public Object getResponse() {
        return response;
    }

    public void setResponse(Object response) {
        this.response = response;
    }

    @Override
    public String toString(){
        return JSONObject.toJSONString(this);
    }

}
