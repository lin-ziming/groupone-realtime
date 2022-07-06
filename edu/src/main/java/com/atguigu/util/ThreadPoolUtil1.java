package com.atguigu.util;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author lzc
 * @Date 2022/6/29 14:16
 */
public class ThreadPoolUtil1 {
    public static Executor getThreadPool() {
        return new ThreadPoolExecutor(
            300,
            400,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(100)
        );
    }
}