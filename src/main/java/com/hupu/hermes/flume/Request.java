package com.hupu.hermes.flume;

import com.google.gson.JsonArray;

public class Request {
    private String body;
    private String ip;
    private String server_time;
    private String request_method;
    /**
     * 兼容之前错误拼写逻辑
     */
    private String requet_method;
    private String ua;
    private String a_key;
    private String kv;

    private String partner;

    private String tbl;


//    private JsonArray partner_body;
//
//
//    public JsonArray getPartner_body() {
//        return partner_body;
//    }
//
//    public void setPartner_body(JsonArray partner_body) {
//        this.partner_body = partner_body;
//    }

    public String getPartner() {
        return partner;
    }

    public void setPartner(String partner) {
        this.partner = partner;
    }

    public String getTbl() {
        return tbl;
    }

    public void setTbl(String tbl) {
        this.tbl = tbl;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getServer_time() {
        return server_time;
    }

    public void setServer_time(String server_time) {
        this.server_time = server_time;
    }

    public String getRequest_method() {
        return request_method;
    }

    public void setRequest_method(String request_method) {
        this.request_method = request_method;
    }

    public String getUa() {
        return ua;
    }

    public void setUa(String ua) {
        this.ua = ua;
    }

    public String getA_key() {
        return a_key;
    }

    public void setA_key(String a_key) {
        this.a_key = a_key;
    }

    public String getKv() {
        return kv;
    }

    public void setKv(String kv) {
        this.kv = kv;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getRequet_method() {
        return requet_method;
    }

    public void setRequet_method(String requet_method) {
        this.requet_method = requet_method;
    }
}
