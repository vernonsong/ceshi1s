package com.example.dtottoentitydemo.mapper;

import com.example.dtottoentitydemo.domain.User;
import com.example.dtottoentitydemo.dto.UserDTO;
import org.mapstruct.Mapper;
import org.mapstruct.MappingTarget;

/**
 * MapStruct Mapper：自动处理DTO到实体的转换，无需手动赋值
 */
@Mapper(componentModel = "spring")
public interface UserMapper {
    
    /**
     * 方法1：DTO直接转换为实体
     * MapStruct会自动使用User.UserBuilder来创建实体
     */
    User toEntity(UserDTO userDTO);
    
    /**
     * 方法2：实体转换为DTO
     * 用于查询场景
     */
    UserDTO toDto(User user);
    
    /**
     * 方法3：将DTO映射到现有实体
     * 用于更新场景
     */
    void updateEntityFromDto(UserDTO userDTO, @MappingTarget User.UserBuilder builder);
}
