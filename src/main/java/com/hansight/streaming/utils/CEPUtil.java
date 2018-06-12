package com.hansight.streaming.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by liujia on 2018/3/22.
 */
public class CEPUtil {
    private static Logger LOG = LoggerFactory.getLogger(CEPUtil.class);

    private static AtomicLong callCount = new AtomicLong(0);

    private static final double EARTH_RADIUS = 6378.137;
    private static double rad(double d) {
        return d * Math.PI / 180.0;
    }
    private static Map<String, Double> distanceMap = new ConcurrentHashMap<>();

    public static double geoDistance(String point1, String point2) {
        if (distanceMap.containsKey(point1+point2)) {
            return distanceMap.get(point1+point2);
        }
        String[] point1Arr = point1.split(",");
        String[] point2Arr = point2.split(",");
        if (point1Arr.length != 2 || point2Arr.length != 2) {
            LOG.warn("Distance calculate failed, because of wrong args: {} {}", point1, point2);
            return 0;
        }
        double lat1 = Double.valueOf(point1Arr[0]);
        double lon1 = Double.valueOf(point1Arr[1]);
        double lat2 = Double.valueOf(point2Arr[0]);
        double lon2 = Double.valueOf(point2Arr[1]);

        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lon1) - rad(lon2);
        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
                + Math.cos(radLat1) * Math.cos(radLat2)
                * Math.pow(Math.sin(b / 2), 2)));
        s = s * EARTH_RADIUS;
        s = Math.round(s * 10000) / 10 / 1000;
        distanceMap.put(point1+point2, s);
        return s;
    }


    public static double geoDistance(double[] point1, double[] point2) {
        String key = String.format("%s, %s", Arrays.toString(point1), Arrays.toString(point2));
        if (distanceMap.containsKey(key)) {
            return distanceMap.get(key);
        }
        System.out.println(callCount.getAndIncrement());
        double lat1 = point1[0];
        double lon1 = point1[1];
        double lat2 = point2[0];
        double lon2 = point2[1];

        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lon1) - rad(lon2);
        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
                + Math.cos(radLat1) * Math.cos(radLat2)
                * Math.pow(Math.sin(b / 2), 2)));
        s = s * EARTH_RADIUS;
        s = Math.round(s * 10000) / 10 / 1000;
        distanceMap.put(key, s);
        return s;
    }

    /**
     * @param left  12:23:32 -> 12*60*60 + 23*60 + 32
     * @param right 19:23:32 -> 19*60*60 + 23*60 + 32
     */
    public static boolean inTimeRange(long left, long right, long occurTime) {
        long value = (occurTime / 1000) % (24 * 60 * 60);
        if (left < right) {
            return left <= value && right >= value;
        } else {
            return left <= value || right >= value;
        }
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        for (int i = 0; i < 10000000; i++) {
            distanceMap.containsKey("123");
        }
        System.out.println(System.currentTimeMillis() - start);
    }
}
