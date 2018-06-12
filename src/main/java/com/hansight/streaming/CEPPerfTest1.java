package com.hansight.streaming;

import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
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
 *
 */
public class CEPPerfTest1 {
    private static final int TIMES_THRESHOLD = 10;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeyedStream<JSONObject, String> keyedStream = env.addSource(new PeriodicSourceFunction(TIMES_THRESHOLD))
                .assignTimestampsAndWatermarks(new PeriodicTimestampExtractor())
                .keyBy((KeySelector<JSONObject, String>) value -> value.getString("user"));

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getString("user").equals("kevin");
                    }
                })
                .timesOrMore(TIMES_THRESHOLD).greedy()
                .within(Time.minutes(20));

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
        // private long pause = 100;
        private int threshold;
        private String[] names = {"kevin", "tony", "wurui", "sujun", "wuhao", "liujia"};
        private String[] event_types = {"logon", "http", "file", "device", "email"};

        // no cancel trigger in this case
        private boolean running = true;

        PeriodicSourceFunction(int eventCountThreshold) {
            this.threshold = eventCountThreshold;
            startTime = System.currentTimeMillis();
        }

        public void run(SourceContext<JSONObject> ctx) throws Exception {
            while (running) {
                if (index.get() % (threshold*100) == 0) {
                    for (int i = 0; i < threshold; i++) {
                        ctx.collect(buildEvent(true));
                    }
                } else {
                    ctx.collect(buildEvent(false));
                }
            }
        }

        /**
         * @param isAnomaly build abnormal event
         */
        private JSONObject buildEvent(boolean isAnomaly) {
            JSONObject ret = new JSONObject();
            ret.put("id", index.getAndIncrement());
            if (isAnomaly) {
                ret.put("user", names[0]);
            } else {
                ret.put("user", names[random(1, 5)]);
            }
            ret.put("event_type", event_types[random(0, 4)]);
            ret.put("event_time", startTime += 1000);
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
