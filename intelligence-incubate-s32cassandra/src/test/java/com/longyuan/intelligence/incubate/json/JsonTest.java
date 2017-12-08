package com.longyuan.intelligence.incubate.json;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mulin on 2017/12/8.
 */
public class JsonTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonTest.class);

    @Test
    public void testEmptyJson() {
        JSONObject json = new JSONObject();
        LOGGER.info("empty json string:{}", json.toJSONString());
        String empty = "{}";
        JSONObject emptyJson = JSONObject.parseObject(empty);
        LOGGER.info("json empty string:{}", emptyJson.toJSONString());


    }
}
