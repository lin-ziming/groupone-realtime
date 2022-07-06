package com.atguigu.util;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("row<kw string>"))
public class IkAnalyzer extends TableFunction<Row> {
    public void eval(String keyword) {
        // 把keyword进行分词
        // 小米手机
        List<String> kws = IkUtil.split(keyword);
        // list有多少个字符串, 就输出多少行
        for (String kw : kws) {
            collect(Row.of(kw));
        }
    }
}
