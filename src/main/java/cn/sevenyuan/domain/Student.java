package cn.sevenyuan.domain;

import lombok.Data;

import java.util.Date;

/**
 * @author JingQ at 2019-09-22
 */
@Data
public class Student {

    private int id;

    private String name;

    private int age;

    private String address;

    private Date checkInTime;

    public Student() {
    }

    public Student(int id, String name, int age, String address) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.address = address;
    }
}
