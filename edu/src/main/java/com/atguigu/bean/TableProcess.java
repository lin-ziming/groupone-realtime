package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author shogunate
 * @description TableProcess for table gmall_config.table_process_edu_dim
 * @date 2022/6/15 19:34
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcess {
    private String sourceTable;
    private String sinkTable;
    private String sinkColumns;
    private String sinkPk;
    private String sinkExtend;

    private String operate_type;
}