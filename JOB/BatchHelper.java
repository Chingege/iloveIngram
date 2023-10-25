/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2018-2023. All rights reserved.
 */

package com.huawei.esop.common.util.batch;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * 批量处理助手
 *
 * @author cwx246028
 * @since 2018-04-30
 */
@Component
@ConditionalOnClass({ SqlSessionTemplate.class })
public class BatchHelper {
    @Value("${mybatis.batch.size:2000}")
    private int batchSize;

    @Autowired
    private SqlSessionTemplate sqlSessionTemplate;

    /**
     * Execute batch *（已淘汰，要求使用下面executeBatch(List data）
     *
     * @param mapper mapper
     * @param methodName method name
     * @param data data
     * @throws IllegalAccessException IllegalAccessException
     * @throws IllegalArgumentException IllegalArgumentException
     * @throws SecurityException SecurityException
     * @throws InvocationTargetException InvocationTargetException
     * @throws NoSuchMethodException NoSuchMethodException
     */
    @Deprecated
    @Transactional(propagation = Propagation.SUPPORTS)
    public void executeBatch(Object mapper, String methodName, List<?> data)
        throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException,
        InvocationTargetException {
        if (data == null || data.isEmpty()) {
            return;
        }
        // 启用批量模式
        SqlSession sqlSession = sqlSessionTemplate.getSqlSessionFactory().openSession(ExecutorType.BATCH);
        // 获取需要执行的方法
        Method method = mapper.getClass().getDeclaredMethod(methodName, data.get(0).getClass());
        int count = 0;
        for (Object obj : data) {
            method.invoke(mapper, obj);
            if (++count == batchSize) {
                sqlSession.flushStatements();
                count = 0;
            }
        }
        sqlSession.flushStatements();
    }

    /**
     * 批量操作数据库
     *
     * @param data data
     * @param mapperClass mapperClass
     * @param consumer consumer
     * @param <T> dateType
     * @param <M> mapperType
     */
    @Transactional(propagation = Propagation.SUPPORTS)
    public <T, M> void executeBatch(List<T> data, Class<M> mapperClass, BiConsumer<M, T> consumer) {
        if (data == null || data.isEmpty()) {
            return;
        }

        // 启用批量模式
        SqlSession sqlSession = sqlSessionTemplate.getSqlSessionFactory().openSession(ExecutorType.BATCH);

        // 获取需要执行的方法
        final M mapper = sqlSession.getMapper(mapperClass);
        int count = 0;
        for (T obj : data) {
            consumer.accept(mapper, obj);
            if (++count == batchSize) {
                sqlSession.flushStatements();
                count = 0;
            }
        }
        sqlSession.flushStatements();
    }
}
