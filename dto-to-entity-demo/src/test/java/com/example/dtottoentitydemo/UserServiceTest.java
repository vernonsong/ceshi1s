package com.example.dtottoentitydemo;

import com.example.dtottoentitydemo.domain.User;
import com.example.dtottoentitydemo.dto.UserDTO;
import com.example.dtottoentitydemo.service.UserService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class UserServiceTest {
    
    @Autowired
    private UserService userService;
    
    /**
     * 测试1：正常情况 - 完整DTO转换为实体
     */
    @Test
    void testCreateUserDirect_Success() {
        // 1. 创建完整的DTO
        UserDTO userDTO = new UserDTO();
        userDTO.setUsername("testuser");
        userDTO.setEmail("test@example.com");
        userDTO.setAge(25);
        userDTO.setPhone("13800138000");
        userDTO.setAddress("123 Main St");
        userDTO.setCity("Shanghai");
        userDTO.setCountry("China");
        userDTO.setActive(true);
        userDTO.setDepartment("IT");
        userDTO.setPosition("Developer");
        
        // 2. 转换并创建实体
        User user = userService.createUserDirect(userDTO);
        
        // 3. 验证转换结果
        assertNotNull(user);
        assertEquals(userDTO.getUsername(), user.getUsername());
        assertEquals(userDTO.getEmail(), user.getEmail());
        assertEquals(userDTO.getAge(), user.getAge());
        assertEquals(userDTO.getPhone(), user.getPhone());
        assertEquals(userDTO.getAddress(), user.getAddress());
        assertEquals(userDTO.getCity(), user.getCity());
        assertEquals(userDTO.getCountry(), user.getCountry());
        assertEquals(userDTO.getActive(), user.getActive());
        assertEquals(userDTO.getDepartment(), user.getDepartment());
        assertEquals(userDTO.getPosition(), user.getPosition());
    }
    
    /**
     * 测试2：正常情况 - 只有关键字段的DTO转换为实体
     * 非关键字段会使用默认值或null
     */
    @Test
    void testCreateUserDirect_MinimalFields() {
        // 1. 创建只包含关键字段的DTO
        UserDTO userDTO = new UserDTO();
        userDTO.setUsername("testuser");
        userDTO.setEmail("test@example.com");
        // 其他字段不设置
        
        // 2. 转换并创建实体
        User user = userService.createUserDirect(userDTO);
        
        // 3. 验证转换结果
        assertNotNull(user);
        assertEquals(userDTO.getUsername(), user.getUsername());
        assertEquals(userDTO.getEmail(), user.getEmail());
        // 非关键字段可以为null，因为我们没有校验它们
        assertNull(user.getAge());
        assertNull(user.getPhone());
        assertNull(user.getAddress());
        assertNull(user.getCity());
        assertNull(user.getCountry());
        assertNull(user.getActive());
        assertNull(user.getDepartment());
        assertNull(user.getPosition());
    }
    
    /**
     * 测试3：错误情况 - 缺少关键字段
     */
    @Test
    void testCreateUserDirect_MissingKeyFields() {
        // 1. 创建缺少关键字段的DTO
        UserDTO userDTO = new UserDTO();
        // 不设置username和email
        userDTO.setAge(25);
        userDTO.setPhone("13800138000");
        
        // 2. 验证会抛出IllegalArgumentException
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            userService.createUserDirect(userDTO);
        });
        
        // 3. 验证错误信息
        assertTrue(exception.getMessage().contains("用户名不能为空"));
        assertTrue(exception.getMessage().contains("邮箱不能为空"));
    }
    
    /**
     * 测试4：错误情况 - 关键字段格式不正确
     */
    @Test
    void testCreateUserDirect_InvalidEmailFormat() {
        // 1. 创建邮箱格式不正确的DTO
        UserDTO userDTO = new UserDTO();
        userDTO.setUsername("testuser");
        userDTO.setEmail("invalid-email"); // 无效邮箱格式
        
        // 2. 验证会抛出IllegalArgumentException
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            userService.createUserDirect(userDTO);
        });
        
        // 3. 验证错误信息
        assertTrue(exception.getMessage().contains("邮箱格式不正确"));
    }
    
    /**
     * 测试5：Builder模式 - 自定义修改
     */
    @Test
    void testCreateUserWithBuilder() {
        // 1. 创建基础DTO
        UserDTO userDTO = new UserDTO();
        userDTO.setUsername("builderuser");
        userDTO.setEmail("builder@example.com");
        userDTO.setAge(30);
        
        // 2. 使用Builder模式创建实体，会设置默认值
        User user = userService.createUserWithBuilder(userDTO);
        
        // 3. 验证转换结果和默认值
        assertNotNull(user);
        assertEquals(userDTO.getUsername(), user.getUsername());
        assertEquals(userDTO.getEmail(), user.getEmail());
        assertEquals(userDTO.getAge(), user.getAge());
        
        // 验证Service中设置的默认值
        assertTrue(user.getActive());
        assertEquals("Engineering", user.getDepartment());
        assertEquals("Beijing", user.getCity());
    }
    
    /**
     * 测试6：从现有实体创建Builder进行更新
     */
    @Test
    void testUpdateUser() {
        // 1. 创建初始实体
        UserDTO initialDto = new UserDTO();
        initialDto.setUsername("initialuser");
        initialDto.setEmail("initial@example.com");
        initialDto.setAge(28);
        User initialUser = userService.createUserDirect(initialDto);
        
        // 2. 创建更新DTO
        UserDTO updateDto = new UserDTO();
        updateDto.setUsername("updateduser");
        updateDto.setEmail("updated@example.com");
        // 只更新关键字段，其他字段保持不变
        
        // 3. 使用toBuilder()进行更新
        User updatedUser = userService.updateUser(initialUser, updateDto);
        
        // 4. 验证更新结果
        assertNotNull(updatedUser);
        assertEquals(updateDto.getUsername(), updatedUser.getUsername());
        assertEquals(updateDto.getEmail(), updatedUser.getEmail());
        assertEquals(initialUser.getAge(), updatedUser.getAge()); // 非关键字段保持不变
    }
    
    /**
     * 测试7：验证Builder的toBuilder功能
     */
    @Test
    void testToBuilderFunctionality() {
        // 1. 创建初始实体
        UserDTO userDTO = new UserDTO();
        userDTO.setUsername("tobuilderuser");
        userDTO.setEmail("tobuilder@example.com");
        userDTO.setAge(35);
        User user = userService.createUserDirect(userDTO);
        
        // 2. 使用toBuilder()创建新的Builder并修改
        User updatedUser = user.toBuilder()
                              .age(36)
                              .city("Guangzhou")
                              .department("Product")
                              .build();
        
        // 3. 验证修改结果
        assertEquals(user.getUsername(), updatedUser.getUsername());
        assertEquals(user.getEmail(), updatedUser.getEmail());
        assertEquals(36, updatedUser.getAge());
        assertEquals("Guangzhou", updatedUser.getCity());
        assertEquals("Product", updatedUser.getDepartment());
    }
}
