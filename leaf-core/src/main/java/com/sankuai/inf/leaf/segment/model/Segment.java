package com.sankuai.inf.leaf.segment.model;

import java.util.concurrent.atomic.AtomicLong;

/**
 * value 就是用来产生id值的，它是一个 AtomicLong 类型，多线程下可以利用它的一些原子API操作。max 则代表自己（号段对象）能产生的最大的id值，
 * 也就是value的上限，用完了就需要切换号段，自己重新从数据库获取下一个号段区间。step 是动态调整的步长，关于动态调整，官方博客也有所解释，这里先不赘述。
 * 当自己用完了，就需要从数据库请求新的号段区间，区间大小就是由这个 step 决定的。
 */
public class Segment {
    /**
     * 内存中生成的每一个id号
     */
    private AtomicLong value = new AtomicLong(0);
    /**
     * 当前号段允许的最大id值
     */
    private volatile long max;
    /**
     * 步长，会根据数据库的step动态调整
     */
    private volatile int step;
    /**
     * 当前号段所属的SegmentBuffer
     */
    private SegmentBuffer buffer;

    public Segment(SegmentBuffer buffer) {
        this.buffer = buffer;
    }

    public AtomicLong getValue() {
        return value;
    }

    public void setValue(AtomicLong value) {
        this.value = value;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public SegmentBuffer getBuffer() {
        return buffer;
    }

    /**
     * 当前号段的剩余量
     *
     * @return 当前号段的剩余量
     */
    public long getIdle() {
        return this.getMax() - getValue().get();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Segment(");
        sb.append("value:");
        sb.append(value);
        sb.append(",max:");
        sb.append(max);
        sb.append(",step:");
        sb.append(step);
        sb.append(")");
        return sb.toString();
    }
}
