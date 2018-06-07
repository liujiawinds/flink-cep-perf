package com.hansight.streaming;

import com.alibaba.fastjson.JSONObject;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Created by liujia on 2018/6/5.
 */
public class PeriodicTimestampExtractor implements AssignerWithPeriodicWatermarks<JSONObject> {
    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(JSONObject element, long previousElementTimestamp) {
        Long timestamp = element.getLong("event_time");
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp);
    }
}
