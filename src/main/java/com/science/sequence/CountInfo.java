package com.science.sequence;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaixiaotong on 2017-1-12.
 */
public class CountInfo {

    private AtomicInteger count;
    private  volatile  boolean status = true;

    public CountInfo(AtomicInteger count, boolean status) {
        this.count = count;
        this.status = status;
    }

    public AtomicInteger getCount() {
        return count;
    }

    public void setCount(AtomicInteger count) {
        this.count = count;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }
}
