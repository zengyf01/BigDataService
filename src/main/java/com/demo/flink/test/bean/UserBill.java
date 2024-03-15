package com.demo.flink.test.bean;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @author zyf
 * @date 2023/8/18
 * @desc 用户账单
 */
@Data
public class UserBill {
    private String id ;
    private String usaerId;
    private BigDecimal amount;
    private Date createTime;
    private Date updateTime;
    private String remark;
}
