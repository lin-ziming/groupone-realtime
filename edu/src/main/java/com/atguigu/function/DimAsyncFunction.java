package com.atguigu.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.concurrent.Executor;

/**
 * @author shogunate
 * @description TODO
 * @date 2022/7/5 19:18
 */
public class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {

    private Executor threadPool;

    @Override
    public void open(Configuration parameters) throws Exception {

    }


    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

    }
}
