package com.example.demo.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.demo.bean.User;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by shanshan on 2020/12/24.
 */
@Repository
@Component(value ="userMapper")
public interface UserMapper extends BaseMapper<User> {
    User getInfo(@Param("name") String name, @Param("password") String password);

    User selectByUserName(@Param("username") String username);

    List<User> selectAll();
}
