package com.demo.flink.test.bean;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * @author zyf
 * @date 2023/5/9
 * @desc 用户实体
 */
public class UserTest implements Serializable {
    //姓名
    private String name;
    //年月
    private Integer time;
    //工时
    private Integer workingHours;
    //工资
    private BigDecimal wages;


    public UserTest() {}

    public UserTest(String name, Integer workingHours, Integer time, BigDecimal wages) {
        this.name = name;
        this.workingHours = workingHours;
        this.time = time;
        this.wages = wages;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getWorkingHours() {
        return workingHours;
    }

    public void setWorkingHours(Integer workingHours) {
        this.workingHours = workingHours;
    }

    public Integer getTime() {
        return time;
    }

    public void setTime(Integer time) {
        this.time = time;
    }

    public BigDecimal getWages() {
        return wages;
    }

    public void setWages(BigDecimal wages) {
        this.wages = wages;
    }

    @Override
    public String toString() {
        return
                "name='" + name + '\'' +
                ", time=" + time +
                ", workingHours=" + workingHours +
                ", wages=" + wages ;
    }
}
