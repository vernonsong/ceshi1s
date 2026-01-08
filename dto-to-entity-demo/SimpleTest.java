import java.util.ArrayList;
import java.util.List;

/**
 * 简单测试类，演示DTO到实体的转换逻辑
 * 无需Maven，直接用javac编译运行
 */
public class SimpleTest {
    
    public static void main(String[] args) {
        System.out.println("=== 简单测试：DTO到实体转换 ===");
        
        // 1. 创建测试DTO
        UserDTO userDTO = new UserDTO();
        userDTO.setUsername("testuser");
        userDTO.setEmail("test@example.com");
        userDTO.setAge(25);
        userDTO.setCity("Shanghai");
        
        System.out.println("1. 原始DTO: " + userDTO);
        
        // 2. 使用Builder模式转换
        User user = User.builder()
                .username(userDTO.getUsername())
                .email(userDTO.getEmail())
                .age(userDTO.getAge())
                .city(userDTO.getCity())
                .build();
        
        System.out.println("2. 转换后的实体: " + user);
        
        // 3. 测试校验
        System.out.println("3. 测试校验...");
        try {
            user.validate();
            System.out.println("   ✅ 校验通过！");
        } catch (IllegalArgumentException e) {
            System.out.println("   ❌ 校验失败：" + e.getMessage());
        }
        
        // 4. 测试缺少关键字段的情况
        System.out.println("4. 测试缺少关键字段...");
        User invalidUser = User.builder()
                .email("invalid")  // 无效邮箱
                .build();
        
        try {
            invalidUser.validate();
            System.out.println("   ✅ 校验通过！");
        } catch (IllegalArgumentException e) {
            System.out.println("   ❌ 校验失败：" + e.getMessage());
        }
        
        // 5. 测试toBuilder功能
        System.out.println("5. 测试toBuilder功能...");
        User updatedUser = user.toBuilder()
                .username("updateduser")
                .city("Beijing")
                .build();
        
        System.out.println("   更新前: " + user);
        System.out.println("   更新后: " + updatedUser);
        
        System.out.println("\n=== 测试完成！ ===");
    }
    
    // 简化版User实体，演示Builder模式和选择性校验
    public static class User {
        // 字段
        private final String username;
        private final String email;
        private final Integer age;
        private final String city;
        
        // 私有构造函数
        private User(Builder builder) {
            this.username = builder.username;
            this.email = builder.email;
            this.age = builder.age;
            this.city = builder.city;
        }
        
        // Getter方法
        public String getUsername() { return username; }
        public String getEmail() { return email; }
        public Integer getAge() { return age; }
        public String getCity() { return city; }
        
        // 校验方法：只校验关键字段
        public void validate() {
            List<String> errors = new ArrayList<>();
            
            if (username == null || username.isEmpty()) {
                errors.add("用户名不能为空");
            }
            
            if (email == null || email.isEmpty()) {
                errors.add("邮箱不能为空");
            } else if (!email.contains("@")) {
                errors.add("邮箱格式不正确");
            }
            
            if (!errors.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errors));
            }
        }
        
        // Builder方法
        public static Builder builder() {
            return new Builder();
        }
        
        // toBuilder方法
        public Builder toBuilder() {
            return new Builder()
                    .username(this.username)
                    .email(this.email)
                    .age(this.age)
                    .city(this.city);
        }
        
        @Override
        public String toString() {
            return "User{username='" + username + "', email='" + email + "', age=" + age + ", city='" + city + "'}";
        }
        
        // Builder内部类
        public static class Builder {
            private String username;
            private String email;
            private Integer age;
            private String city;
            
            public Builder username(String username) {
                this.username = username;
                return this;
            }
            
            public Builder email(String email) {
                this.email = email;
                return this;
            }
            
            public Builder age(Integer age) {
                this.age = age;
                return this;
            }
            
            public Builder city(String city) {
                this.city = city;
                return this;
            }
            
            public User build() {
                return new User(this);
            }
        }
    }
    
    // 简化版UserDTO
    public static class UserDTO {
        private String username;
        private String email;
        private Integer age;
        private String city;
        
        // Getter和Setter方法
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }
        public Integer getAge() { return age; }
        public void setAge(Integer age) { this.age = age; }
        public String getCity() { return city; }
        public void setCity(String city) { this.city = city; }
        
        @Override
        public String toString() {
            return "UserDTO{username='" + username + "', email='" + email + "', age=" + age + ", city='" + city + "'}";
        }
    }
}
