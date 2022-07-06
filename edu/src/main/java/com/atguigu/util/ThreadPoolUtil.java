package com.atguigu.util;

import org.eclipse.jetty.util.thread.ExecutorThreadPool;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author shogunate
 * @description TODO
 * @date 2022/7/5 19:47
 */
public class ThreadPoolUtil {
    private ThreadPoolUtil() {
    }

    private static ThreadPoolUtil instance = null;

    public static ThreadPoolUtil getThreadPoolInstance() {
        if (instance == null) {
            synchronized (ThreadPoolUtil.class) {
                if (instance == null) {
                    instance = new ThreadPoolUtil();
                }
            }
        }
        return instance;
    }

    private final Executor executor = new ExecutorThreadPool(
        300,
        400,
        60,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(100));

    public void executor(Runnable runnable) {
        executor.execute(runnable);
    }

}













