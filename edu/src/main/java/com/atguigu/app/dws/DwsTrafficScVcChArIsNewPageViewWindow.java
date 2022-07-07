package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.BaseAppV2;
import com.atguigu.bean.TrafficPageViewBean;
import com.atguigu.function.DimAsyncFunction;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.atguigu.common.Constant.*;

public class DwsTrafficScVcChArIsNewPageViewWindow extends BaseAppV2 {
    public static void main(String[] args) {

//        HashMap<String, Long[]> topicsOffsets = new HashMap<>();
//        topicsOffsets.put(TOPIC_DWD_TRAFFIC_PAGE, new Long[]{500000L, 400000L});
//        topicsOffsets.put(TOPIC_DWD_TRAFFIC_UV, new Long[]{1000L, 3000L});
//        topicsOffsets.put(TOPIC_DWD_TRAFFIC_UJ_DETAIL, new Long[]{300L, 200L});

        new DwsTrafficScVcChArIsNewPageViewWindow().init(
                3002,
                2,
                "DwsTrafficScVcChArIsNewPageViewWindow",
//                topicsOffsets,
                TOPIC_DWD_TRAFFIC_PAGE
                ,TOPIC_DWD_TRAFFIC_UV
                ,TOPIC_DWD_TRAFFIC_UJ_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       Map<String, DataStreamSource<String>> streams) {


        // 1. 解析, 把多个流union成一个流
        DataStream<TrafficPageViewBean> beanStream = unionOne(streams);
        // 2. join dim
        SingleOutputStreamOperator<TrafficPageViewBean> streamWithDim = joinDim(beanStream);
        // 3. 开窗聚合
        SingleOutputStreamOperator<TrafficPageViewBean> resultStream = windowAndAggregate(streamWithDim);
        // 4. 写出到clickhouse中
        writeToClickHouse(resultStream);
    }

    private SingleOutputStreamOperator<TrafficPageViewBean> joinDim(DataStream<TrafficPageViewBean> stream) {
        return AsyncDataStream.unorderedWait(
                stream,
                new DimAsyncFunction<TrafficPageViewBean>() {
                    @Override
                    public String getTable() {
                        return "dim_base_source";
                    }

                    @Override
                    public String getId(TrafficPageViewBean bean) {
                        return bean.getSc();
                    }

                    @Override
                    public void addDim(TrafficPageViewBean bean, JSONObject dim) {
                        bean.setSourceSite(dim.getString("SOURCE_SITE"));
                        bean.setSourceUrl(dim.getString("SOURCE_URL"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
    }

    private void writeToClickHouse(SingleOutputStreamOperator<TrafficPageViewBean> resultStream) {
        resultStream.addSink(FlinkSinkUtil.getClickHoseSink("dws_traffic_rc_vc_ch_ar_is_new_page_view_window", TrafficPageViewBean.class));
    }

    private SingleOutputStreamOperator<TrafficPageViewBean> windowAndAggregate(SingleOutputStreamOperator<TrafficPageViewBean> stream) {
        return stream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(15)) //多个流有两个流路径较长
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                )
                .keyBy(bean -> bean.getSc() + "_" + bean.getCh() + "_" + bean.getAr() + "_" + bean.getIsNew() + "_" + bean.getVc())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(new OutputTag<TrafficPageViewBean>("late") {
                })
                .reduce(
                        new ReduceFunction<TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean reduce(TrafficPageViewBean bean1,
                                                              TrafficPageViewBean bean2) throws Exception {
                                bean1.setUvCt(bean1.getUvCt() + bean2.getUvCt());
                                bean1.setSvCt(bean1.getSvCt() + bean2.getSvCt());
                                bean1.setPvCt(bean1.getPvCt() + bean2.getPvCt());
                                bean1.setDurSum(bean1.getDurSum() + bean2.getDurSum());
                                bean1.setUjCt(bean1.getUjCt() + bean2.getUjCt());

                                return bean1;
                            }
                        },
                        new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                Context ctx,
                                                Iterable<TrafficPageViewBean> elements,
                                                Collector<TrafficPageViewBean> out) throws Exception {
                                TrafficPageViewBean bean = elements.iterator().next();
                                String stt = DateFormatUtil.toYmdHms(ctx.window().getStart());
                                String edt = DateFormatUtil.toYmdHms(ctx.window().getEnd());

                                bean.setStt(stt);
                                bean.setEdt(edt);

                                // ts改成统计时间
                                bean.setTs(ctx.currentProcessingTime());

                                out.collect(bean);

                            }
                        }
                );
    }

    private DataStream<TrafficPageViewBean> unionOne(Map<String, DataStreamSource<String>> streams) {
//        streams.get(TOPIC_DWD_TRAFFIC_UJ_DETAIL).print(TOPIC_DWD_TRAFFIC_UJ_DETAIL);

        // pv sv  durSum
        SingleOutputStreamOperator<TrafficPageViewBean> pvSvDurSumStream = streams
                .get(TOPIC_DWD_TRAFFIC_PAGE)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    JSONObject common = obj.getJSONObject("common");
                    JSONObject page = obj.getJSONObject("page");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");
                    String sc = common.getString("sc");

                    Long pv = 1L;
                    Long sv = page.getString("last_page_id") == null ? 1L : 0L;
                    Long durSum = page.getLong("during_time");

                    Long ts = obj.getLong("ts");

                    return new TrafficPageViewBean("", "",
                            sc, "", "",
                            vc, ch, ar, isNew,
                            0L, sv, pv, durSum, 0L,
                            ts
                    );
                });
        // uv
        SingleOutputStreamOperator<TrafficPageViewBean> uvStream = streams
                .get(TOPIC_DWD_TRAFFIC_UV)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    JSONObject common = obj.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");
                    String sc = common.getString("sc");

                    Long ts = obj.getLong("ts");

                    return new TrafficPageViewBean("", "",
                            sc, "", "",
                            vc, ch, ar, isNew,
                            1L, 0L, 0L, 0L, 0L,
                            ts
                    );
                });

        // uv
        SingleOutputStreamOperator<TrafficPageViewBean> ujStream = streams
                .get(TOPIC_DWD_TRAFFIC_UJ_DETAIL)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    JSONObject common = obj.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");
                    String sc = common.getString("sc");

                    Long ts = obj.getLong("ts");

                    return new TrafficPageViewBean("", "",
                            sc, "", "",
                            vc, ch, ar, isNew,
                            0L, 0L, 0L, 0L, 1L,
                            ts
                    );
                });

        return pvSvDurSumStream.union(uvStream, ujStream);

    }
}
/*
uv和uj数据迟到, 为什么?





不同的维度:


五个指标:
会话数
    页面日志
浏览总时长
    页面日志

页面浏览数  pv
    页面日志
独立访客数  uv
    来源uv详情表
跳出会话数  uj
    来源uj详情表
------------------------

3个流:
union

--------

小米..     1   0   0
小米..     0   1   0
小米..     1   0   0


聚合:
按照维度开窗聚合
  keyBy().window().reduce()...
-----------------------------------
维度    窗口    pv  uv  uj
小米..  0-5     10   3   1
华为..  0-5     20   13   3
 */