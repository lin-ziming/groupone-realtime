package com.atguigu.util;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author shogunate
 * @description TODO
 * @date 2022/7/5 16:58
 */

public class ThreadPoolUtil {

    private ThreadPoolUtil(){}
    private volatile static ThreadPoolUtil threadPoolUtil;
    public static ThreadPoolUtil getThreadPoolInstance(){
        if (threadPoolUtil == null){
            synchronized (ThreadPoolUtil.class){
                if (threadPoolUtil == null){
                    threadPoolUtil = new ThreadPoolUtil();
                }
            }
        }
        return threadPoolUtil;
    }

    public Executor getThreadPool(){
        return new ThreadPoolExecutor(
            300,
            400,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(100)
        );
    }
}
