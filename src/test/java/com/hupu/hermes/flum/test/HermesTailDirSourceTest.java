package com.hupu.hermes.flum.test;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class HermesTailDirSourceTest {

    @Test
    public void postExpload() throws Exception {
        Gson gson = new Gson();
        String body =
                "H4sIAAAAAAAAAO2X24rbRhiAXyX4OnJHoznmzkdIIXTBuSiUIsbS2JqsLAkd7E1DIX2HXPTwBoHQ270pfZltSd+i/8i7a2nti1wUF8zsxTLz+5//PPOh794N6h8GLwajyqivFonK1okyg+eDzaqsQGyukjzTsFcZ7JKmaOx6C2s+DIaYwk7Xgxc+5QEjUgrpU/F8YGq9MTEoFXlVhwGiAUWcgW5VtWJM0JzgOeejYDQbzfjcn8o5Y0wEZCopH4NmlNbWyYhP/LmYezM5Jx4ZT7E3kkHgBZxOfcymUzEnoFysQffq5XiBEMKwb5rWzReeziubjw/pDO1hfQOe3/0IaRYG5MgmHNlg9A2k05S6PWJr880ClnGmNhp2XzfrRKsY/kB43Voc2rOZrc9gZ1a2qmoJpgdLVZkoLHWUbzY6i8NM796U9odtG3aAkATdrUqhsBDHclmFSV5jclDBzEcPKsiqtBZNVuuyyqNIl2Fqqrpj0mfBgz5+1I9UFulURVHeZB1lLCTp+W+Vm2JdqljXueoa9inGx7qQUFNBECrVZdcwDfhxFCqLy9zE4U4vt0bvOral4Me231RhAT3opCZORNBLLSzsEGfNZql7Zca8V0Moc6mL9G1q1kkdrtS17hb8SU9aL4UuNwZmOs+epuoThrvG77uS0LDW/cZI+qAHo3Yfxj6CamfqKOnEINGJvuzHKIcxatsPoxobkPXKjnuxR6mJrlXcCQIfGk7uQyhMVEV5qcPGdKtA/eMI6qSEwe93BW54z6e2se1vyiGs/piNVZqGk/EofK3VZlGruuqO5MGxLecaLJU6jvNemqI/XZDFToHbFRiuVmm+65WSnRiaxGTq5P3BTIjjyT0ufWcAGOuVqtWA1oe1WnfHpF/QLYh1vtI6Dpub7k1g6Gn6bUV7l5YeVECnaN+o19b8/jUMpM8JQTaPqn4LAlUU7dv2+JSZqoD13e2nv3774/NPH/55/ysI01bXuo/vX2+0WiHoXexxJZFHFKaepAx5MeE8ZoqCI/vYLNNrUB6/miBkgzDxavvlb3Jlo1ogf/jwqg/3hoZtQps81ukjnZ59u3j2St3sfSgQT8SIIimYR/Bk7BEOKzESM0/M/MkciymF6bRWdK1sAeGfD+HCwaW9gMYW4fOfv/z98+93t+/vbj++vALxf41JIU9gEn6m2GHSYdJh0mHSYfKMmMSXhkl8GZiUp74mmZSIOkw6TDpMOkw6TJ4Rk8GlYTK4CExidOprkkuEkcOkw6TDpMOkw+QZMUkuDZPkMjDpn/qa5Byidph0mHSYdJh0mDwjJumlYZJeBiYxP4FJ4TPiMOkw6TDpMOkweU5MskvDJPtfMPn9v/hUkxbrKgAA";
        byte[] bytes = Base64.decodeBase64(body.replace("\\", "\\\\"));
        InputStream input = new GZIPInputStream(new ByteArrayInputStream(bytes));
        Reader reader = new InputStreamReader(input);
        JsonArray jsonElements = gson.fromJson(reader, JsonArray.class);
        for (JsonElement s : jsonElements) {
            JsonObject asJsonObject = s.getAsJsonObject();
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("a", 1);
            map.put("b", "b");
            asJsonObject.add("meta", gson.toJsonTree(map));
            asJsonObject.addProperty("ip", "这是一个IP");
            System.out.println(gson.toJson(asJsonObject).replace("\"", "\\\""));
        }
        System.out.println(gson.toJson(jsonElements).replace("\"", "\\\""));
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>");
    }

    //@Test
    public void getExpload() throws Exception {
        Gson gson = new Gson();
        String body = "H4sIAAAAAAAAAO3cTWvcRhgA4L8SdK13mS+NpL15Q1p6SAjYh0IPYlaa9arWSkIfa7uhP6CHhhZKoc0pUEovvTXQS+mfSdzkX/QdybvWVxzXSXAIL/ignXl3NF+ah9FK/vKRpYLSmlkqy4pS5aW1ZxXmc5DGIRxrOKa2wwml0uaOYHtWXJ5BPoFMlYRRCMdCsgXjtiuCQIcBWUJWtNZRAVmutB0iifAYdwnlJqfIIP35X3+8ePL3vz8+e/7PU0g83kDahsLRWgVwKMlsfz6TzuyePbvnzKhncpa5KfGLSKXryHxOQx1Dwv3P73h3Du5BSmry95MwT6Ow/mhK9eoWnTVNhOPyazj87P7hJ8SdEdOKqnpdI9TCmj2yFovCP1GlzpcqjotlnJ6Y1E39JeYRCYEbBRWh3+zVsVkUFEGaa7+KWoHSpp1AVUSBn6hN5EdBmlwGUinFNpDtAqMEzl+kULXcjyMzQNt4TiVvx2sTmai1bp3b9nYhZFdkoJJAxyoI0iop/WyVJjqp1gudt8omzO23LtdZfBZHR6vSX6rj9lkk8frBTVxxEpXBqtNpYqQv9ElVQPNUrPOyXXnutNu3gWSdLrUO/eq01W2eJO02XvTVyvZL3e0uz97GwRyGSBX6hVZ5u4KcXY6Bxa1dFctVriE8y9vNFr1mN527ihI1Ol5MunI4GLkO0vU6TcJmpOGSDCNI6/QDGznP8HudmdSZcnUEdIZfqqNWlBibmJ3J0aqE63VG7gjmWa7DMO1U1HWGE/hidI/gWmt1HnXlyMxUzRXsn+jFJtIn7UG+LPqyrl8V3RHh1GXDqEzn66goojTpzzAKV367EqZR9VXUabccFlllR7kKdZmq9gBTm3VOP4dVw7873/cPtVoflKos2uXarH/NrNKSifZ8oa3xMfPVrI+rKquqLIRVqV6GIeXlz4/Pv/3BfDKLnjPlU+ZMYdFmZrovwmahlq5gLpfE9cx1HdRpDqOccmnaF8Tl+EIYQsfVlVnaNqeUThY6UBPhLOlEMcebOK6jFNFOyEKz8CZGDYsfmcW1+Z4joVuoud623gRxFByb6mbRhSaL+BiO5ocPCDEt1uabEwrzttTrupSJSc5g7loP9x/OSROW1Yv+oenGwvTMAaHTbfa0KWxa53Ylg4kHSaf1uMVqUUPyYL5vrnX0DX1D39A39A19u7lvMLd0UYwAVysWVtD7VPZxi7tGeWzn3cHdC+9ayDVp04n5M13Y/bIjB+Z5Q/MQPAQPwUPwEDwE772Dx+w3gWcPN3jDXd0WvKK6LoWkT6FHHXQP3UP30D10D917z+7ZLhVOi74shcnLCax4xOFdBW1oE+M7BecH4JroKtikjSnY87GloE2Y7G0IzZk42W0Ii7TKA5h81qvffnr19Nn5499ffvc97g5RSVQSlUQlUck3KSlu+HPfp9f5uW/n4MXPfXyUxKawaZ3bls52mPAGtz7Pn/z54tdfkDgkDolD4pA4JO7tiNOngFOV6zHl7t8lpKZidAvoS484r8eu92zLFru6zMGzLbYtpItPaaJpaBqahqahabdlGqxnzd3N/7eFu1BtsIUT3PVQNVQNVUPVUDVU7aPYqQmBj6GgaWgamoamoWkfy06NugRVQ9VQNVQNVUPVbks1xxNcvF618acqt6qJgWqEClQNVUPVUDVUDVW7PdVc94o7kPJK1WRfNe4xvAOJqqFqqBqqhqq9lWrXeBFOXPclOCHNq2k3egmul7PpFsv6L8EJKWx8CQ6FRCFRSBQShfxw933v9G4md3Hfh6qhaqgaqoaq3ZpqLnHlFarZV6pm91XjntneoWqoGqqGqqFqqNotqcaaPn4nz1MKz+OoGqqGqqFqqBqq9qE+edL7X8091eiIavg8JaqGqqFqqBqqdiPV/gOQxl3QvXQAAA==";
        System.out.println(">>>" + body.length());

        String body2 = "H4sIAAAAAAAAAO3cTWvcRhgA4L8SdK13mS+NpL15Q1p6SAjYh0IPYlaa9arWSkIfa7uhP6CHhhZKoc0pUEovvTXQS+mfSdzkX/QdybvWVxzXSXAIL/ignXl3NF+ah9FK/vKRpYLSmlkqy4pS5aW1ZxXmc5DGIRxrOKa2wwml0uaOYHtWXJ5BPoFMlYRRCMdCsgXjtiuCQIcBWUJWtNZRAVmutB0iifAYdwnlJqfIIP35X3+8ePL3vz8+e/7PU0g83kDahsLRWgVwKMlsfz6TzuyePbvnzKhncpa5KfGLSKXryHxOQx1Dwv3P73h3Du5BSmry95MwT6Ow/mhK9eoWnTVNhOPyazj87P7hJ8SdEdOKqnpdI9TCmj2yFovCP1GlzpcqjotlnJ6Y1E39JeYRCYEbBRWh3+zVsVkUFEGaa7+KWoHSpp1AVUSBn6hN5EdBmlwGUinFNpDtAqMEzl+kULXcjyMzQNt4TiVvx2sTmai1bp3b9nYhZFdkoJJAxyoI0iop/WyVJjqp1gudt8omzO23LtdZfBZHR6vSX6rj9lkk8frBTVxxEpXBqtNpYqQv9ElVQPNUrPOyXXnutNu3gWSdLrUO/eq01W2eJO02XvTVyvZL3e0uz97GwRyGSBX6hVZ5u4KcXY6Bxa1dFctVriE8y9vNFr1mN527ihI1Ol5MunI4GLkO0vU6TcJmpOGSDCNI6/QDGznP8HudmdSZcnUEdIZfqqNWlBibmJ3J0aqE63VG7gjmWa7DMO1U1HWGE/hidI/gWmt1HnXlyMxUzRXsn+jFJtIn7UG+LPqyrl8V3RHh1GXDqEzn66goojTpzzAKV367EqZR9VXUabccFlllR7kKdZmq9gBTm3VOP4dVw7873/cPtVoflKos2uXarH/NrNKSifZ8oa3xMfPVrI+rKquqLIRVqV6GIeXlz4/Pv/3BfDKLnjPlU+ZMYdFmZrovwmahlq5gLpfE9cx1HdRpDqOccmnaF8Tl+EIYQsfVlVnaNqeUThY6UBPhLOlEMcebOK6jFNFOyEKz8CZGDYsfmcW1+Z4joVuoud623gRxFByb6mbRhSaL+BiO5ocPCDEt1uabEwrzttTrupSJSc5g7loP9x/OSROW1Yv+oenGwvTMAaHTbfa0KWxa53Ylg4kHSaf1uMVqUUPyYL5vrnX0DX1D39A39A19u7lvMLd0UYwAVysWVtD7VPZxi7tGeWzn3cHdC+9ayDVp04n5M13Y/bIjB+Z5Q/MQPAQPwUPwEDwE772Dx+w3gWcPN3jDXd0WvKK6LoWkT6FHHXQP3UP30D10D917z+7ZLhVOi74shcnLCax4xOFdBW1oE+M7BecH4JroKtikjSnY87GloE2Y7G0IzZk42W0Ii7TKA5h81qvffnr19Nn5499ffvc97g5RSVQSlUQlUck3KSlu+HPfp9f5uW/n4MXPfXyUxKawaZ3bls52mPAGtz7Pn/z54tdfkDgkDolD4pA4JO7tiNOngFOV6zHl7t8lpKZidAvoS484r8eu92zLFru6zMGzLbYtpItPaaJpaBqahqahabdlGqxnzd3N/7eFu1BtsIUT3PVQNVQNVUPVUDVU7aPYqQmBj6GgaWgamoamoWkfy06NugRVQ9VQNVQNVUPVbks1xxNcvF618acqt6qJgWqEClQNVUPVUDVUDVW7PdVc94o7kPJK1WRfNe4xvAOJqqFqqBqqhqq9lWrXeBFOXPclOCHNq2k3egmul7PpFsv6L8EJKWx8CQ6FRCFRSBQShfxw933v9G4md3Hfh6qhaqgaqoaq3ZpqLnHlFarZV6pm91XjntneoWqoGqqGqqFqqNotqcaaPn4nz1MKz+OoGqqGqqFqqBqq9qE+edL7X8091eiIavg8JaqGqqFqqBqqdiPV/gOQxl3QvXQAAA==";
        System.out.println(">>>" + body2.length());

        byte[] bytes = Base64.decodeBase64(body2);
        JsonElement jsonElement = gson.fromJson(new InputStreamReader(new ByteArrayInputStream(bytes)), JsonElement.class);
        System.out.println(jsonElement.getAsJsonObject());
    }

    @Test
    public void createSign() {
        Long ts = System.currentTimeMillis();
        String str = "24v223250r6EFWCqi534354vCXAtnbuusKwn_hupu_themis_up_up_" + "1585023797759";
        String s = DigestUtils.md5Hex(str.getBytes()) + "_" + "1585023797759";
        System.out.println("sign: " + s);
    }

    @Test
    public void mockADX() {
        HashMap<String, Object> re = Maps.newHashMap();
        re.put("act", "click");
        re.put("event", "广告下载");
        HashMap<String, Object> ext = Maps.newHashMap();
        HashMap<String, Object> ad = Maps.newHashMap();
        re.put("ext", ext);
        ext.put("ad", ad);
        ad.put("pos", "广告位ID");
        ad.put("brand", "品牌ID");
        ad.put("pkg", "包名");
        ad.put("req", "request_id");
        ad.put("srv", "server_ip");
        ad.put("slot", "slot_id");
        ad.put("type", "广告分类");
        ad.put("union", "广告联盟的名字(头条、广点通)");
        ad.put("title", "标题");
        ad.put("req_no", "请求数");
        ad.put("source", "前项来源");
        ad.put("list_no", "广告位数");
        ad.put("exp_type", "曝光类型");
        re.put("itemid", "topic_开头为话题");
        re.put("av", "app_version");
        re.put("uid", "user_id/puid");
        re.put("et", System.currentTimeMillis());
        re.put("clt", "client_code(字母数字组成)");
        re.put("ab", "libra_ab");
        re.put("cid", "client_id(业务系统用的纯数字)");
        re.put("idfa", "idfa");
        re.put("andid", "android_id");
        re.put("imeis", "imeis");
        Gson gson = new Gson();
        String s = gson.toJson(re);
        System.out.println(s);
    }

    @Test
    public void testMap() throws Exception {
        Date date = DateUtils.addDays(DateUtils.parseDate("20200207", new String[]{"yyyyMMdd"}), -18299);
        System.out.println(DateFormatUtils.format(date, "yyyyMMdd"));
    }

}
