package com.github.txxs.zklock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.InetAddress;

/**
 * @Author:jianghuimin
 * @Date: 2017/7/27
 * @Time:11:25
 */
@Component
@Aspect
public class DistributedLockAspect {

    public final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DistributedLockAspect.class);

    @Value("${zk.servers}")
    private String zkServers;

    private CuratorFramework curatorFramework;

    @PostConstruct
    public void init() {
        try {
            curatorFramework = CuratorFrameworkFactory.builder().connectString(zkServers)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
            curatorFramework.start();
            curatorFramework.blockUntilConnected();
            curatorFramework.checkExists().creatingParentContainersIfNeeded();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("error", e);
        }
    }

    @Around("@annotation(com.lecloud.cdn.custoppo.zklock.DistributedLock)")
    public Object validate(final ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        final DistributedLock distributedLock = ((MethodSignature) proceedingJoinPoint.getSignature())
                .getMethod().getAnnotation(DistributedLock.class);
        final String path = distributedLock.path();
        final String methodName = ((MethodSignature) proceedingJoinPoint.getSignature()).getMethod().getName();
        final InterProcessMutex mutex = new InterProcessMutex(curatorFramework, path);
        final String hostAddress = InetAddress.getLocalHost().getHostAddress();
        try {
            LOGGER.info("start:{} and the methed:{} acquire the lock", hostAddress,methodName);
            mutex.acquire();
            LOGGER.info("info:{} and the methed:{} acquire the lock success", hostAddress,methodName);
            return proceedingJoinPoint.proceed();
        } finally {
            mutex.release();
            LOGGER.info("end:{} and the methed:{}release the lock success", hostAddress,methodName);
        }
    }
}

