package com.atguigu.function;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

/**
 * @author shogunate
 * @description TODO
 * @date 2022/7/5 19:18
 */
public class DimAsyncFunction<T> extends RichAsyncFunction {

    @Override
    public void asyncInvoke(Object input, ResultFuture resultFuture) throws Exception {

    }
}
