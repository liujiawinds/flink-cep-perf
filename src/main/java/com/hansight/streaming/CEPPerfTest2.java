package com.hansight.streaming;

import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by liujia on 2018/6/5.
 */
public class CEPPerfTest2 {
    private static final int TIMES_THRESHOLD = 10;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeyedStream<JSONObject, String> keyedStream = env.addSource(new PeriodicSourceFunction(TIMES_THRESHOLD))
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<JSONObject>() {
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
                }).keyBy((KeySelector<JSONObject, String>) value -> value.getString("user"));

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return true;
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
        private AtomicLong userIndex = new AtomicLong();
        private long startTime;
        // private long pause = 100;
        private int threshold;
        private List<String> users = new ArrayList<>(1003);
        private String[] event_types = {"logon", "http", "file", "device", "email"};

        // no cancel trigger in this case
        private boolean running = true;

        PeriodicSourceFunction(int eventCountThreshold) {
            this.threshold = eventCountThreshold;
            startTime = System.currentTimeMillis();
        }

        public void run(SourceContext<JSONObject> ctx) throws Exception {
            while (running) {
                if ((index.get() % 100) == 0) {
                    for (int i = 0; i < threshold; i++) {
                        ctx.collect(buildEvent());
                    }
                    userIndex.incrementAndGet();
                } else {
                    for (int i = 0; i < threshold - 1; i++) {
                        ctx.collect(buildEvent());
                    }
                    userIndex.incrementAndGet();
                }
            }
        }

        private JSONObject buildEvent() {
            JSONObject ret = new JSONObject();
            ret.put("id", index.getAndIncrement());
            ret.put("user", users.get(Math.toIntExact(userIndex.get() % users.size())));
            ret.put("event_type", event_types[random(0, 4)]);
            ret.put("event_time", startTime += 5 * 1000);
            return ret;
        }

        private int random(int min, int max) {
            Random random = new Random();
            return random.nextInt(max) % (max - min + 1) + min;
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            // change path when deploy

            List<String> lines = Files.readAllLines(Paths.get("/home/sonice/users.csv"));
//            List<String> lines = Files.readAllLines(Paths.get(ClassLoader.getSystemResource("users.csv").toURI()));
            for (int i = 1; i < lines.size(); i++) {
                String line = lines.get(i);
                String[] fields = line.split(",", -1);
                users.add(fields[1]);
            }
        }

        public void cancel() {
            running = false;
        }
    }
}
