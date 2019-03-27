package com.wupj.bd.common;

/**
 * 〈sqoop回调〉
 *
 * @author wupeiji
 * @create 2019/3/22
 * @since 1.0.0
 */
public abstract class SqoopCallback {
    public abstract void succes();
    public  abstract void fail(String msg);
    public abstract void update(double progress);
}
