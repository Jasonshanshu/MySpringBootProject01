package com.example.demo.service;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.example.demo.bean.User;
import com.example.demo.mapper.UserMapper;

/**
 * Created by shanshan on 2020/12/24.
 */
public abstract class UserService extends ServiceImpl<UserMapper, User> {
    public abstract User loginIn(String name, String password);
}
