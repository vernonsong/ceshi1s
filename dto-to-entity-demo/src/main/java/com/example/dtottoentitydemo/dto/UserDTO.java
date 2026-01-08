package com.example.dtottoentitydemo.dto;

import lombok.Data;

@Data
public class UserDTO {
    // DTO字段与实体字段对应
    private Long id;
    private String username;
    private String email;
    private Integer age;
    private String phone;
    private String address;
    private String city;
    private String country;
    private Boolean active;
    private String department;
    private String position;
    // 字段与实体保持一致，用于数据传输
}
