package com.demo.flink.test.bean;

import com.alibaba.fastjson2.PropertyNamingStrategy;
import com.alibaba.fastjson2.annotation.JSONType;
import lombok.Data;

/**
 * @author zyf
 * @date 2023/8/19
 * @desc
 */
@Data
@JSONType(naming= PropertyNamingStrategy.SnakeCase)
public class CdcResultBO {
    /**
     * 数据操作类型：c = 新增、d = 删除（改主键或删除）、u = 更新、r = 查询(初始化)
     */
    private String op;

    /**
     * 表名称
     */
    private CdcResultSourceBO source;

    /**
     * 变化之前
     */
    private Object before;

    /**
     * 变化之后
     */
    private Object after;

    /**
     * 时间
     */
    private Long tsMs;

}


