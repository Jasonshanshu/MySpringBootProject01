package com.example.demo.Util;

/**
 * Created by shanshan on 2020/12/26.
 */
public enum StatusEnum {
    OPRATION_SUCCESS("200", "操作成功"),
    OPRATION_FAILED("401", "操作失败"),
    EXIST("101", "数据存在"),
    NOT_EXIST("402", "数据不存在");

    StatusEnum(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    private String code;
    private String msg;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
