package com.demo.flink.test.bean;

import lombok.Data;

/**
 * @author zyf
 * @date 2023/8/18
 * @desc 用户
 */
@Data
public class User {
    private String id ;
    private String name;
    private Integer age;
    private String address;
}
