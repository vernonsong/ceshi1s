package com.example.dtottoentitydemo.domain;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

@Getter
@Builder(toBuilder = true)
@ToString
public class User {
    // 领域实体字段
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
    // 更多字段可以继续添加，无需手写Builder

    /**
     * 选择性校验：只对关键字段进行校验
     * 非关键字段（如age、phone、address等）不进行校验，可使用默认值
     */
    public void validate() {
        List<String> errors = new ArrayList<>();
        
        // 只校验关键字段
        if (StringUtils.isBlank(username)) {
            errors.add("用户名不能为空");
        }
        
        if (StringUtils.isBlank(email)) {
            errors.add("邮箱不能为空");
        } else if (!email.contains("@")) {
            errors.add("邮箱格式不正确");
        }
        
        // 非关键字段不校验，或使用默认值处理
        // age、phone、address等字段可以为null或默认值
        
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(String.join(", ", errors));
        }
    }
}
