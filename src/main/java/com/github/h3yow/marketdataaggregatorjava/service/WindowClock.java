package com.github.h3yow.marketdataaggregatorjava.service;

/**
 * 窗口归属与边界规则（全系统唯一）。
 *
 * <p>固定大小窗口、左闭右开：
 * <pre>
 *     start = floor(ts / size) * size
 *     窗口 = [start, start + size)
 * </pre>
 * 使用 {@link Math#floorDiv(long, long)} / {@link Math#floorMod(long, long)}，
 * 对负时间戳同样得到“不晚于 ts 的最大 size 整数倍”，规则无歧义。
 */
public final class WindowClock {

    private WindowClock() {
    }

    /**
     * 返回时间戳所属窗口的起点（含）。
     */
    public static long windowStart(long timestamp, long windowSizeMillis) {
        return timestamp - Math.floorMod(timestamp, windowSizeMillis);
    }

    /**
     * 时间戳是否落在 [windowStart, windowStart + size) 内。
     */
    public static boolean inWindow(long timestamp, long windowStart, long windowSizeMillis) {
        return timestamp >= windowStart && timestamp < windowStart + windowSizeMillis;
    }

    /**
     * 下一窗口起点。
     */
    public static long nextWindowStart(long windowStart, long windowSizeMillis) {
        return windowStart + windowSizeMillis;
    }
}
