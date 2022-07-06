package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV1;
import com.atguigu.bean.InteractionVideoPlayBean;
import com.atguigu.common.Constant;
import com.atguigu.function.DimAsyncFunction;
import com.atguigu.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author shogunate
 * @description join dim table video_info on video_id
 * @date 2022/7/4 19:55
 */
public class DWSInteractionVideoPlayWindow extends BaseAppV1 {

    public static void main(String[] args) {
        new DWSInteractionVideoPlayWindow().init(11042, 2, "DWDInteractionVideoPlay", Constant.TOPIC_DWD_TRAFFIC_APPVIDEO);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //distinct uid per day and parse pojo
        SingleOutputStreamOperator<InteractionVideoPlayBean> beanStream = distinctUIDAndParsePojo(stream);
//        beanStream.print();

        //要先dim join chapter_info 获取维度后才聚合, keyby chapter_id
        SingleOutputStreamOperator<InteractionVideoPlayBean> withDimStream = joinDimVideoInfo(beanStream);
        withDimStream.print();


//        SingleOutputStreamOperator<InteractionVideoPlayBean> aggStream = WindowAllAndAggregate(withDimStream);
//        aggStream.print();


    }

    private SingleOutputStreamOperator<InteractionVideoPlayBean> joinDimVideoInfo(SingleOutputStreamOperator<InteractionVideoPlayBean> beanStream) {
        SingleOutputStreamOperator<InteractionVideoPlayBean> joinDimVideoInfo = AsyncDataStream.unorderedWait(beanStream,
            new DimAsyncFunction<InteractionVideoPlayBean>() {
                @Override
                public String getTable() {
                    return "dim_video_info";
                }

                @Override
                public String getId(InteractionVideoPlayBean input) {
                    return input.getVideoId();
                }

                @Override
                public void addDim(InteractionVideoPlayBean input, JSONObject dim) {
                    input.setChapterId(dim.getString("CHAPTER_ID"));
                }
            },
            60,
            TimeUnit.SECONDS);

        return AsyncDataStream.unorderedWait(joinDimVideoInfo,
            new DimAsyncFunction<InteractionVideoPlayBean>() {
                @Override
                public String getTable() {
                    return "dim_chapter_info";
                }

                @Override
                public String getId(InteractionVideoPlayBean input) {
                    return input.getChapterId();
                }

                @Override
                public void addDim(InteractionVideoPlayBean input, JSONObject dim) {
                    input.setChapterName(dim.getString("CHAPTER_NAME"));
                }
            },
            60,
            TimeUnit.SECONDS);
    }

    private SingleOutputStreamOperator<InteractionVideoPlayBean> WindowAllAndAggregate(SingleOutputStreamOperator<InteractionVideoPlayBean> stream) {
        return stream.assignTimestampsAndWatermarks(WatermarkStrategy
            .<InteractionVideoPlayBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner((bean, ts) -> bean.getTs()))
            .keyBy(InteractionVideoPlayBean::getChapterId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(new ReduceFunction<InteractionVideoPlayBean>() {
                        @Override
                        public InteractionVideoPlayBean reduce(InteractionVideoPlayBean value1, InteractionVideoPlayBean value2) throws Exception {
                            value1.getUserId().addAll(value2.getUserId());
                            value1.setPlayCount(value1.getPlayCount() + value2.getPlayCount());
                            value1.setPlaySecSum(value1.getPlaySecSum() + value2.getPlaySecSum());
                            return value1;
                        }
                    },
                new ProcessWindowFunction<InteractionVideoPlayBean, InteractionVideoPlayBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<InteractionVideoPlayBean> elements, Collector<InteractionVideoPlayBean> out) throws Exception {
                        InteractionVideoPlayBean bean = elements.iterator().next();

                        bean.setStt(DateFormatUtil.toYmdHms(context.window().getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(context.window().getEnd()));
                        bean.setTs(context.currentProcessingTime());
                        bean.setViewerCount((long) bean.getVideoId().length());
                        bean.setAvgSecPerViewer(bean.getPlaySecSum() / bean.getViewerCount());
                    }
                }
            );
    }

//            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
//            .reduce(new ReduceFunction<InteractionVideoPlayBean>() {
//                        @Override
//                        public InteractionVideoPlayBean reduce(InteractionVideoPlayBean value1, InteractionVideoPlayBean value2) throws Exception {
//
////                            value1.setViewerCount(value1.getViewerCount() + value2.getViewerCount());
//                            value1.getUserId().addAll(value2.getUserId());
//                            value1.setPlayCount(value1.getPlayCount() + value2.getPlayCount());
//                            value1.setPlaySecSum(value1.getPlaySecSum() + value2.getPlaySecSum());
//                            return value1;
//                        }
//                    },
//                new AllWindowFunction<InteractionVideoPlayBean, InteractionVideoPlayBean, TimeWindow>() {
//                    @Override
//                    public void apply(TimeWindow window, Iterable<InteractionVideoPlayBean> values, Collector<InteractionVideoPlayBean> out) throws Exception {
//                        InteractionVideoPlayBean bean = values.iterator().next();
//
//                        bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
//                        bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
//                        bean.setTs(System.currentTimeMillis());
//                        bean.setViewerCount((long) bean.getUserId().size());
//                        bean.setAvgSecPerViewer(Double.valueOf((new DecimalFormat("0.0000").format(bean.getPlaySecSum() / bean.getViewerCount()))));
//
//                        out.collect(bean);
//                    }
//                }
//            );


    private SingleOutputStreamOperator<InteractionVideoPlayBean> distinctUIDAndParsePojo(DataStreamSource<String> stream) {

        return stream.map(JSONObject::parseObject)
            .keyBy(json -> json.getJSONObject("common").getString("uid"))
            .process(new KeyedProcessFunction<String, JSONObject, InteractionVideoPlayBean>() {

                @Override
                public void processElement(JSONObject value, Context ctx, Collector<InteractionVideoPlayBean> out) throws Exception {
                    JSONObject appVideo = value.getJSONObject("appVideo");

                    //直接统计
                    InteractionVideoPlayBean bean = InteractionVideoPlayBean.builder()
                        .playSecSum(appVideo.getDouble("play_sec"))
                        .videoId(appVideo.getString("video_id"))
                        .playCount(1L)
                        .ts(value.getLong("ts"))
                        .build();

                    //观看人数去重 放入set, 而不是依靠状态来判断, 因为5s窗口内状态值仍为0, 除0出异常

                    //only count once per day
//                        System.out.println(value.getJSONObject("common").getString("uid"));

                    bean.getUserId().add(value.getJSONObject("common").getString("uid"));

                    out.collect(bean);


                }
            });
    }

}