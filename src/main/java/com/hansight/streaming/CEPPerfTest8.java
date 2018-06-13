package com.hansight.streaming;

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
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by liujia on 2018/6/5.
 */
public class CEPPerfTest8 {

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


    private static class PeriodicSourceFunction extends RichParallelSourceFunction<JSONObject> {
        private AtomicLong index = new AtomicLong();
        private long startTime;
        private String[] names = {"kevin", "tony", "wurui", "sujun", "wuhao", "liujia", "liujia1", "liujia2", "liujia3"};
        // no cancel trigger in this case
        private boolean running = true;

        PeriodicSourceFunction() {
            startTime = System.currentTimeMillis();
        }

        public void run(SourceContext<JSONObject> ctx) throws Exception {
            while (running) {
                ctx.collect(buildEvent());
            }
        }

        private JSONObject buildEvent() {
            JSONObject ret = new JSONObject();
            ret.put("id", index.getAndIncrement());
            ret.put("user", "liujia");
            ret.put("geo", new double[]{30.5129375, 105.4405871});
            ret.put("event_time", startTime += 6 * 1000);
            ret.put("event_type", "logon");
            return ret;
        }

        private int random(int min, int max) {
            Random random = new Random();
            return random.nextInt(max) % (max - min + 1) + min;
        }

        public void cancel() {
            running = false;
        }
    }
}