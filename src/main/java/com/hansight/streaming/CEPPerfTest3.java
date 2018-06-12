package com.hansight.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hansight.streaming.utils.CEPUtil;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by liujia on 2018/6/5.
 */
public class CEPPerfTest3 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeyedStream<JSONObject, String> keyedStream = env.addSource(new PeriodicSourceFunction())
                .assignTimestampsAndWatermarks(new PeriodicTimestampExtractor())
                .keyBy((KeySelector<JSONObject, String>) value -> value.getString("user"));

        // move more than 100km within 10 minutes
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("prev")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject event) throws Exception {
                        return event.getString("event_type").equals("logon");
                    }
                })
                .followedBy("curr")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject currentEvent, Context<JSONObject> ctx) throws Exception {
                        if (!currentEvent.getString("event_type").equals("logon")) {
                            return false;
                        }
                        Iterable<JSONObject> iterator = ctx.getEventsForPattern("prev");
                        JSONObject previousEvent = null;
                        for (JSONObject jsonObject : iterator) {
                            previousEvent = jsonObject;
                        }
                        return CEPUtil.geoDistance(previousEvent.getString("geo"), currentEvent.getString("geo")) > 100;
                    }
                })
                .within(Time.minutes(10));

        CEP.pattern(keyedStream, pattern)
                .select(new PatternSelectFunction<JSONObject, List<JSONObject>>() {

                    @Override
                    public List<JSONObject> select(Map<String, List<JSONObject>> pattern) throws Exception {
                        return null;
                    }
                });
        env.execute();
    }


    private static class PeriodicSourceFunction extends RichSourceFunction<JSONObject> {
        private AtomicLong index = new AtomicLong();
        private long startTime;
        // no cancel trigger in this case
        private boolean running = true;

        PeriodicSourceFunction() {
            startTime = System.currentTimeMillis();
        }

        public void run(SourceContext<JSONObject> ctx) throws Exception {
            while (running) {
                if ((index.get() % 100) == 0) {
                    ctx.collect(buildEvent(true));
                    ctx.collect(buildEvent(true));
                } else {
                    ctx.collect(buildEvent(false));
                }
            }
        }

        private JSONObject buildEvent(boolean isAnomaly) {
            JSONObject ret = new JSONObject();
            ret.put("id", index.getAndIncrement());
            ret.put("user", "liujia");
            if (isAnomaly) {
                // 遂宁
                ret.put("geo", "30.5129375,105.4405871");
                ret.put("event_time", startTime += 5 * 1000);
            } else {
                // 成都高新地铁站
                ret.put("geo", "30.5959627,104.0565345");
                ret.put("event_time", startTime += 10 * 1000 * 60);
            }
            ret.put("event_type", "logon");
            return ret;
        }

        public void cancel() {
            running = false;
        }
    }
}
