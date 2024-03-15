package com.demo.flink.test.bean;

import lombok.Data;

/**
 * @author zyf
 * @date 2023/8/19
 * @desc
 */
@Data
public class CdcResultSourceBO {
    private String db;
    private String table;
}
