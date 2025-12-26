package org.example.util;

public class ConvertUtil {
    public static long parseLongFast(String s) {
        long v = 0;
        for (int i = 0; i < s.length(); i++) v = v * 10 + (s.charAt(i) - '0');
        return v;
    }

    public static double parseDoubleFast(String s) {
        long intPart = 0, fracPart = 0, div = 1;
        boolean dot = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '.') {
                dot = true;
                continue;
            }
            int d = c - '0';
            if (!dot) intPart = intPart * 10 + d;
            else {
                fracPart = fracPart * 10 + d;
                div *= 10;
            }
        }
        return intPart + (fracPart / (double) div);
    }

    public static long usedMemoryBytes() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }
}
