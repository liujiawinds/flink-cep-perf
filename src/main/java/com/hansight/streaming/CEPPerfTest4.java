package com.hansight.streaming;

import com.alibaba.fastjson.JSONObject;
import com.hansight.streaming.utils.CEPUtil;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by liujia on 2018/6/5.
 */
public class CEPPerfTest4 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeyedStream<Event, String> keyedStream = env.addSource(new PeriodicSourceFunction())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Event>() {
                    private long currentMaxTimestamp;

                    @Override
                    public long extractTimestamp(Event element, long previousElementTimestamp) {
                        Long timestamp = element.getEventTime();
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        return timestamp;
                    }

                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxTimestamp - (60 * 1000 * 10));
                    }
                })
                .keyBy(Event::getUser);

        // move more than 100km within 10 minutes
        Pattern<Event, Event> pattern = Pattern.<Event>begin("prev")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getEventType().equals("logon");
                    }
                })
                .followedBy("curr")
                .where(new IterativeCondition<Event>() {
                    @Override
                    public boolean filter(Event currentEvent, Context<Event> ctx) throws Exception {
                        if (!currentEvent.getEventType().equals("logon")) {
                            return false;
                        }
                        Iterable<Event> iterator = ctx.getEventsForPattern("prev");
                        Event previousEvent = null;
                        for (Event jsonObject : iterator) {
                            previousEvent = jsonObject;
                        }
                        return !Arrays.equals(previousEvent.getGeo(), currentEvent.getGeo()) && CEPUtil.geoDistance(previousEvent.getGeo(), currentEvent.getGeo()) > 100;
                    }
                })
                .within(Time.minutes(10));

        CEP.pattern(keyedStream, pattern)
                .select(new PatternSelectFunction<Event, List<Event>>() {
                    @Override
                    public List<Event> select(Map<String, List<Event>> pattern) throws Exception {
                        return null;
                    }
                });
        env.execute();
    }


    private static class PeriodicSourceFunction extends RichParallelSourceFunction<Event> {
        private AtomicLong index = new AtomicLong();
        private long startTime;
        private String[] names = {"kevin", "tony", "wurui", "sujun", "wuhao", "liujia", "liujia1", "liujia2", "liujia3"};
        // no cancel trigger in this case
        private boolean running = true;

        PeriodicSourceFunction() {
            startTime = System.currentTimeMillis();
        }

        public void run(SourceContext<Event> ctx) throws Exception {
            while (running) {
                if ((index.get() % 1000) == 0) {
                    ctx.collect(buildEvent(true));
                } else {
                    ctx.collect(buildEvent(false));
                }
            }
        }

        private Event buildEvent(boolean isAnomaly) {
            Event ret = new Event();
            ret.setId(index.getAndIncrement());
            ret.setUser(names[random(0,9)]);
            ret.setEventTime(startTime += 6 * 1000);
            ret.setEventType("logon");
            if (isAnomaly) {
                // 遂宁
                ret.setGeo(new double[]{30.5129375, 105.4405871});
            } else {
                // 成都高新地铁站
                ret.setGeo(new double[]{30.5959627, 104.0565345});
            }
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

    private static class Event {
        private long id;
        private String user;
        private double[] geo;
        private long eventTime;
        private String eventType;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public double[] getGeo() {
            return geo;
        }

        public void setGeo(double[] geo) {
            this.geo = geo;
        }

        public long getEventTime() {
            return eventTime;
        }

        public void setEventTime(long eventTime) {
            this.eventTime = eventTime;
        }

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Event event = (Event) o;

            if (id != event.id) return false;
            if (eventTime != event.eventTime) return false;
            if (user != null ? !user.equals(event.user) : event.user != null) return false;
            if (!Arrays.equals(geo, event.geo)) return false;
            return eventType != null ? eventType.equals(event.eventType) : event.eventType == null;

        }

        @Override
        public int hashCode() {
            int result = (int) (id ^ (id >>> 32));
            result = 31 * result + (user != null ? user.hashCode() : 0);
            result = 31 * result + Arrays.hashCode(geo);
            result = 31 * result + (int) (eventTime ^ (eventTime >>> 32));
            result = 31 * result + (eventType != null ? eventType.hashCode() : 0);
            return result;
        }
    }
}