package com.hansight.streaming.utils;

/**
 * Created by liujia on 2018/3/22.
 */
public class CEPUtil {

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
}
