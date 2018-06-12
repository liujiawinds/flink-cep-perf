package com.hansight.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

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
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.lucene.spatial.util.GeoDistanceUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by liujia on 2018/6/5.
 */
public class CEPPerfTest5 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeyedStream<JSONObject, String> keyedStream = env.addSource(new PeriodicSourceFunction())
                .assignTimestampsAndWatermarks(new PeriodicTimestampExtractor())
                .keyBy((KeySelector<JSONObject, String>) value -> value.getString("user"));

        // move more than 100km within 10 minutes
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject event) throws Exception {
                        return event.getString("user").equals("liujia");
                    }
                });

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
        // no cancel trigger in this case
        private boolean running = true;

        PeriodicSourceFunction() {
            startTime = System.currentTimeMillis();
        }

        public void run(SourceContext<JSONObject> ctx) throws Exception {
            while (running) {
                if ((index.get() % 100) == 0) {
                    ctx.collect(buildEvent(true));
                } else {
                    ctx.collect(buildEvent(false));
                }
            }
        }

        private JSONObject buildEvent(boolean isAnomaly) {
            JSONObject ret = new JSONObject();
            ret.put("id", index.getAndIncrement());
            if (isAnomaly) {
                ret.put("user", "liujia");
            } else {
                ret.put("user", "kevin");
            }
            ret.put("event_time", startTime += 5 * 1000);
            ret.put("event_type", "logon");
            return ret;
        }

        public void cancel() {
            running = false;
        }
    }
}
