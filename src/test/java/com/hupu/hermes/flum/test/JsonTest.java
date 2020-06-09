package com.hupu.hermes.flum.test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.junit.Test;

public class JsonTest {

    //    @Test
    public void t() {

        String s = "";
        String content = new String(s.getBytes()).replace("\\", "\\\\");
        System.out.println(content);

        Gson gson = new Gson();

        JsonObject jsonObject = gson.fromJson(content, JsonObject.class);

        System.out.println(jsonObject.get("ip"));
    }
}
