package com.example.dtottoentitydemo.service;

import com.example.dtottoentitydemo.domain.User;
import com.example.dtottoentitydemo.dto.UserDTO;
import com.example.dtottoentitydemo.mapper.UserMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UserService {
    
    @Autowired
    private UserMapper userMapper;
    
    /**
     * 方式1：直接转换 + 校验
     * 适合大部分场景，简洁高效
     */
    public User createUserDirect(UserDTO userDTO) {
        // 1. 使用MapStruct将DTO直接转换为实体（内部自动使用Builder）
        User user = userMapper.toEntity(userDTO);
        
        // 2. 调用实体的校验方法，只校验关键字段
        user.validate();
        
        // 3. 保存到数据库或进行其他业务操作
        return user;
    }
    
    /**
     * 方式2：Builder模式 + 自定义修改 + 校验
     * 适合需要在构建过程中进行自定义修改的场景
     */
    public User createUserWithBuilder(UserDTO userDTO) {
        // 1. 创建Builder实例
        User.UserBuilder builder = User.builder();
        
        // 2. 使用MapStruct将DTO映射到Builder
        userMapper.updateEntityFromDto(userDTO, builder);
        
        // 3. 在构建过程中进行自定义修改
        // 非关键字段可以在这里设置默认值或覆盖DTO的值
        builder.active(true)  // 设置默认值
               .department("Engineering")  // 设置默认部门
               .city("Beijing");  // 设置默认城市
        
        // 4. 构建实体
        User user = builder.build();
        
        // 5. 调用校验
        user.validate();
        
        return user;
    }
    
    /**
     * 方式3：从现有实体创建Builder进行更新
     * 适合更新场景
     */
    public User updateUser(User existingUser, UserDTO updateDto) {
        // 1. 从现有实体创建Builder
        User.UserBuilder builder = existingUser.toBuilder();
        
        // 2. 将更新DTO映射到Builder
        userMapper.updateEntityFromDto(updateDto, builder);
        
        // 3. 构建新实体
        User updatedUser = builder.build();
        
        // 4. 验证更新后的实体
        updatedUser.validate();
        
        return updatedUser;
    }
    
    /**
     * 方式4：查询场景 - 实体转DTO
     */
    public UserDTO getUserDto(User user) {
        return userMapper.toDto(user);
    }
}
