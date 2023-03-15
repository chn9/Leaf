package com.sankuai.inf.leaf.segment;

import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.segment.dao.IDAllocDao;
import com.sankuai.inf.leaf.segment.model.*;
import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class SegmentIDGenImpl implements IDGen {
    private static final Logger logger = LoggerFactory.getLogger(SegmentIDGenImpl.class);

    /**
     * IDCache未初始化成功时的异常码
     */
    private static final long EXCEPTION_ID_IDCACHE_INIT_FALSE = -1;
    /**
     * key不存在时的异常码
     */
    private static final long EXCEPTION_ID_KEY_NOT_EXISTS = -2;
    /**
     * SegmentBuffer中的两个Segment均未从DB中装载时的异常码
     */
    private static final long EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL = -3;
    /**
     * 最大步长不超过100,0000
     */
    private static final int MAX_STEP = 1000000;
    /**
     * 一个Segment维持时间为15分钟
     */
    private static final long SEGMENT_DURATION = 15 * 60 * 1000L;
    /**
     * 线程池，用于执行异步任务，比如异步准备双buffer中的另一个buffer
     */
    private ExecutorService service = new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new UpdateThreadFactory());
    private volatile boolean initOK = false;
    /**
     * cache，存储所有业务key对应双buffer号段，所以是基于内存的发号方式
     */
    private Map<String, SegmentBuffer> cache = new ConcurrentHashMap<String, SegmentBuffer>();
    private IDAllocDao dao;

    public static class UpdateThreadFactory implements ThreadFactory {

        private static int threadInitNumber = 0;

        private static synchronized int nextThreadNum() {
            return threadInitNumber++;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "Thread-Segment-Update-" + nextThreadNum());
        }
    }

    @Override
    public boolean init() {
        logger.info("Init ...");
        // 确保加载到kv后才初始化成功
        updateCacheFromDb();
        initOK = true;
        // 定时每分钟同步一次db和cache
        updateCacheFromDbAtEveryMinute();
        return initOK;
    }

    private void updateCacheFromDbAtEveryMinute() {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("check-idCache-thread");
                t.setDaemon(true);
                return t;
            }
        });
        service.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                updateCacheFromDb();
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    /**
     * 讲所有数据库中的tags同步到cache中
     */
    private void updateCacheFromDb() {
        logger.info("update cache from db");
        StopWatch sw = new Slf4JStopWatch();
        try {
            // 获取数据库中的所有bizTag
            List<String> dbTags = dao.getAllTags();
            if (dbTags == null || dbTags.isEmpty()) {
                return;
            }
            // 获取当前的cache中的所有tag
            List<String> cacheTags = new ArrayList<String>(cache.keySet());
            Set<String> insertTagsSet = new HashSet<>(dbTags);
            Set<String> removeTagsSet = new HashSet<>(cacheTags);

            /**
             * 下面两步操作：保证cache和数据库tag同步
             * 1. cache新增上数据库后添加的tags
             * 2. cache删除掉数据库表后删除的tags
             */

            // 1. db中新加的tags灌进cache，并实例化初始对应的SegmentBuffer
            for(int i = 0; i < cacheTags.size(); i++){
                String tmp = cacheTags.get(i);
                if(insertTagsSet.contains(tmp)){
                    insertTagsSet.remove(tmp);
                }
            }
            for (String tag : insertTagsSet) {
                /**
                 * 可以看到对于 SegmentBuffer 我们仅仅设置了key，然后就是依靠 SegmentBuffer 自身的构造函数对其内部成员进行了默认初始化，也可以说是零值初始化。
                 * 特别注意，此时 SegmentBuffer 的 initOk 标记还是 false，这也说明这个标记其实并不是标记零值初始化是否完成。然后程序接着对0号 Segment 的所有成员进行了零值初始化。
                 * 同步完成后，即将数据库中的所有 tags 记录加载到内存后，便将ID生成器的初始化标记设置为 true。
                 */
                SegmentBuffer buffer = new SegmentBuffer();
                buffer.setKey(tag);
                // 零值初始化当前正在使用的Segment号段
                Segment segment = buffer.getCurrent();
                segment.setValue(new AtomicLong(0));
                segment.setMax(0);
                segment.setStep(0);
                cache.put(tag, buffer);
                logger.info("Add tag {} from db to IdCache, SegmentBuffer {}", tag, buffer);
            }
            // 2. cache中已失效的tags从cache删除
            for(int i = 0; i < dbTags.size(); i++){
                String tmp = dbTags.get(i);
                if(removeTagsSet.contains(tmp)){
                    removeTagsSet.remove(tmp);
                }
            }
            for (String tag : removeTagsSet) {
                cache.remove(tag);
                logger.info("Remove tag {} from IdCache", tag);
            }
        } catch (Exception e) {
            logger.warn("update cache from db exception", e);
        } finally {
            sw.stop("updateCacheFromDb");
        }
    }

    /**
     * 首先先从 cache 中获取 key 对应的 SegmentBuffer，然后判断 SegmentBuffer 是否是初始化完成，也就是 SegmentBuffer 的 initOk 标记。
     * 这里用了双重判断+synchronized 方式确保 SegmentBuffer 只被初始化一次。那么这里初始化究竟是指什么，才算初始化完成呢？
     *
     * ① 初始化SegmentBuffer
     * 第一部分的逻辑按照调用该方法的次数分为第一次准备号段、第二次准备号段和第三次及之后的准备号段。
     *
     * 第一次准备号段，也就是 SegmentBuffer 还没有DB初始化，我们要从数据库获取一个号段，记录 SegmentBuffer 的当前步长、最小步长都是数据库设置的步长；
     * 第二次准备号段，也就是双buffer的异步准备另一个号段 Segment 时，会进入这一逻辑分支。仍然从数据库获取一个号段，此时记录这次获取下一个号段的时间戳，设置最小步长是数据库设置的步长；
     * 之后再次准备号段，首先要动态调整这次申请号段的区间大小，也就是代码中的 nextStep，调整规则主要跟号段申请频率有关，具体可以查看注释以及代码。计算出动态调整的步长，需要根据新的步长去数据库申请号段，同时记录这次获取号段的时间戳，保存动态调整的步长到 SegmentBuffer，设置最小步长是数据库设置的步长。
     * 第二部分逻辑主要是准备 Segment 号段，将 Segment 号段的四个成员变量进行新一轮赋值，value 就是 id（start=max_id-step）。
     *
     * ② 从号段中获取id
     * 当 SegmentBuffer 和 其中一个号段 Segment 准备好，就可以进行从号段中获取id。我们具体查看号段ID生成器 SegmentIDGenImpl 的 getIdFromSegmentBuffer() 方法。
     *
     * @param key key
     * @return 获取id
     */
    @Override
    public Result get(final String key) {
        // 必须在 SegmentIDGenImpl初始化后执行init方法
        // 也就是必须将数据库中的tags加载到内存cache中，并开启定时同步任务
        if (!initOK) {
            return new Result(EXCEPTION_ID_IDCACHE_INIT_FALSE, Status.EXCEPTION);
        }
        if (cache.containsKey(key)) {
            // 获取cache中对应的SegmentBuffer，SegmentBuffer中包含双buffer，两个号段
            SegmentBuffer buffer = cache.get(key);
            // 双重判断，避免多线程重复执行SegmentBuffer的初始化操作
            // 在get id前检查是否完成DB数据初始化cache中key对应的SegmentBuffer（之前只是零值初始化），需要保证线程安全
            if (!buffer.isInitOk()) {
                synchronized (buffer) {
                    if (!buffer.isInitOk()) {
                        // DB数据初始化SegmentBuffer
                        try {
                            // 根据数据库表中key对应的记录 来初始化SegmentBuffer当前正在使用的Segment
                            updateSegmentFromDb(key, buffer.getCurrent());
                            logger.info("Init buffer. Update leafkey {} {} from db", key, buffer.getCurrent());
                            buffer.setInitOk(true);
                        } catch (Exception e) {
                            logger.warn("Init buffer {} exception", buffer.getCurrent(), e);
                        }
                    }
                }
            }
            // SegmentBuffer准备好之后正常就直接从cache中生成id即可
            return getIdFromSegmentBuffer(cache.get(key));
        }
        return new Result(EXCEPTION_ID_KEY_NOT_EXISTS, Status.EXCEPTION);
    }

    /**
     * 根据数据库表中key对应的记录 来初始化SegmentBuffer当前正在使用的Segment
     * @param key key
     * @param segment segment
     */
    public void updateSegmentFromDb(String key, Segment segment) {
        StopWatch sw = new Slf4JStopWatch();
        /**
         * 1. 先设置 SegmentBuffer
         */
        SegmentBuffer buffer = segment.getBuffer();
        LeafAlloc leafAlloc;
        // 如果buffer没有DB数据初始化（也就是第一次进行DB数据初始化）
        if (!buffer.isInitOk()) {
            // 更新数据库key对应的的最大id值（maxId表示当前分配到的最大id，maxId=maxId+step），并查询更新后的记录返回LeafAlloc
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);
            // 数据库初始设置的step赋值给当前buffer的初始step，后面动态调整
            buffer.setStep(leafAlloc.getStep());
            // leafAlloc中的step为数据库设置的step，buffer这里是未进行DB数据初始化的，所以DB中step代表动态调整的最小下限
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc中的step为DB中的step
        }
        // 如果buffer的更新时间是0（初始是0，也就是第二次调用updateSegmentFromDb()）
        else if (buffer.getUpdateTimestamp() == 0) {
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);
            // 记录buffer的更新时间
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            buffer.setStep(leafAlloc.getStep());
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc中的step为DB中的step
        }
        // 第三次以及之后的进来 动态设置nextStep
        else {
            // 计算当前更新操作和上一次更新时间差
            long duration = System.currentTimeMillis() - buffer.getUpdateTimestamp();
            int nextStep = buffer.getStep();

            /**
             *  动态调整step
             *  1) duration < 15 分钟 : step 变为原来的2倍， 最大为 MAX_STEP
             *  2) 15分钟 <= duration < 30分钟 : nothing
             *  3) duration >= 30 分钟 : 缩小step, 最小为DB中配置的step
             *
             *  这样做的原因是认为15min一个号段大致满足需求
             *  如果updateSegmentFromDb()速度频繁(15min多次)，也就是
             *  如果15min这个时间就把step号段用完，为了降低数据库访问频率，我们可以扩大step大小
             *  相反如果将近30min才把号段内的id用完，则可以缩小step
             */

            // duration < 15 分钟 : step 变为原来的2倍. 最大为 MAX_STEP
            if (duration < SEGMENT_DURATION) {
                if (nextStep * 2 > MAX_STEP) {
                    //do nothing
                } else {
                    nextStep = nextStep * 2;
                }
            } else if (duration < SEGMENT_DURATION * 2) {
                //do nothing with nextStep
            } else {
                nextStep = nextStep / 2 >= buffer.getMinStep() ? nextStep / 2 : nextStep;
            }
            logger.info("leafKey[{}], step[{}], duration[{}mins], nextStep[{}]", key, buffer.getStep(), String.format("%.2f",((double)duration / (1000 * 60))), nextStep);

            /**
             * 根据动态调整的nextStep更新数据库相应的maxId
             */
            // 为了高效更新记录，创建一个LeafAlloc，仅设置必要的字段的信息
            LeafAlloc temp = new LeafAlloc();
            temp.setKey(key);
            temp.setStep(nextStep);
            // 根据动态调整的step更新数据库的maxId
            leafAlloc = dao.updateMaxIdByCustomStepAndGetLeafAlloc(temp);
            // 记录更新时间
            buffer.setUpdateTimestamp(System.currentTimeMillis());
            // 记录当前buffer的动态调整的step值
            buffer.setStep(nextStep);
            // leafAlloc的step为DB中的step，所以DB中的step值代表着下限
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc的step为DB中的step
        }

        /**
         * 2. 准备当前Segment号段
         */

        // 设置Segment号段id的起始值，value就是id（start=max_id-step）
        long value = leafAlloc.getMaxId() - buffer.getStep();
        // must set value before set max
        segment.getValue().set(value);
        segment.setMax(leafAlloc.getMaxId());
        segment.setStep(buffer.getStep());
        sw.stop("updateSegmentFromDb", key + " " + segment);
    }

    /**
     * 首先该方法最外层套了一个循环，不断地尝试获取id。整个方法的逻辑大致包含：
     *
     * 1. 首先获取共享读锁，多个线程能够同时进来获取id。如果能够不需要异步准备双buffer的另一个 Segment 且分发的id号没有超出maxId，那么可以直接返回id号。
     * 多个线程并发获取id号，靠 AtomicLong 的 getAndIncrement() 原子操作保证不出问题。
     * 2. 如果需要异步准备另一个 Segment，则将准备任务提交到线程池中进行完成。多线程执行下，要保证只有一个线程去提交任务。这一点是靠 SegmentBuffer 中的 threadRunning 字段实现的。
     * threadRunning 字段用 volatile 修饰保证多线程可见性，其含义代表了异步准备号段任务是否已经提交线程池运行，是否有其他线程已经开始进行另外号段的初始化工作。使用CAS操作进行更新，
     * 保证 SegmentBuffer 在任意时刻只会有一个线程进行异步更新另外一个号段。
     * 3. 如果号段分配的 id 号超出了maxId，则需要进行切换双buffer的操作。在进行直接切换之前，需要再次判断是否 id 还大于 maxId，因为多线程下，号段已经被其他线程切换成功，自己还不知道，
     * 所以为了避免重复切换出错，需要再次判断。切换操作为了保证同一时间只能有一个线程切换，这里利用了独占式的写锁。
     * @param buffer buffer
     * @return 获取id
     */
    public Result getIdFromSegmentBuffer(final SegmentBuffer buffer) {
        // 自旋获取id
        while (true) {
            // 获取buffer的共享读锁，在平时不操作Segment的情况下益于并发
            buffer.rLock().lock();
            try {
                // 获取当前正在使用的Segment
                final Segment segment = buffer.getCurrent();
                // ===============异步准备双buffer的另一个Segment==============
                // 1. 另一个Segment没有准备好
                // 2. 当前Segment已经使用超过10%则开始异步准备另一个Segment
                // 3. buffer中的threadRunning字段. 代表是否已经提交线程池运行，是否有其他线程已经开始进行另外号段的初始化工作.使用CAS进行更新保证buffer在任意时刻,只会有一个线程进行异步更新另外一个号段.
                if (!buffer.isNextReady() && (segment.getIdle() < 0.9 * segment.getStep()) && buffer.getThreadRunning().compareAndSet(false, true)) {
                    service.execute(new Runnable() {
                        @Override
                        public void run() {
                            // 获得另一个Segment对象
                            Segment next = buffer.getSegments()[buffer.nextPos()];
                            boolean updateOk = false;
                            try {
                                updateSegmentFromDb(buffer.getKey(), next);
                                updateOk = true;
                                logger.info("update segment {} from db {}", buffer.getKey(), next);
                            } catch (Exception e) {
                                logger.warn(buffer.getKey() + " updateSegmentFromDb exception", e);
                            } finally {
                                // 如果准备成功，则通过独占写锁设置另一个Segment准备标记OK，threadRunning为false表示准备完毕
                                if (updateOk) {
                                    // 读写锁是不允许线程先获得读锁继续获得写锁，这里可以是因为这一段代码其实是线程池线程去完成的，不是获取到读锁的线程（是线程池中的其他线程，不是当前线程）
                                    buffer.wLock().lock();
                                    buffer.setNextReady(true);
                                    buffer.getThreadRunning().set(false);
                                    buffer.wLock().unlock();
                                } else {
                                    // 失败了，则还是没有准备好，threadRunning恢复false，以便于下次获取id时重新再异步准备Segment
                                    buffer.getThreadRunning().set(false);
                                }
                            }
                        }
                    });
                }
                // 原子value++(返回旧值)，也就是下一个id，这一步是多线程操作的，每一个线程加1都是原子的，但不一定保证顺序性
                long value = segment.getValue().getAndIncrement();
                // 如果获取到的id小于maxId
                if (value < segment.getMax()) {
                    return new Result(value, Status.SUCCESS);
                }
            } finally {
                // 释放读锁
                buffer.rLock().unlock();
            }

            // 等待线程池异步准备号段完毕
            waitAndSleep(buffer);
            // 获取独占式写锁
            buffer.wLock().lock();
            // 执行到这里，说明当前号段已经用完，应该切换另一个Segment号段使用
            try {
                // 获取当前使用的Segment号段
                final Segment segment = buffer.getCurrent();
                // 重复获取value, 多线程执行时，Segment可能已经被其他线程切换。再次判断, 防止重复切换Segment
                long value = segment.getValue().getAndIncrement();
                if (value < segment.getMax()) {
                    return new Result(value, Status.SUCCESS);
                }
                // 执行到这里, 说明其他的线程没有进行Segment切换，并且当前号段所有号码用完，需要进行切换Segment
                // 如果准备好另一个Segment，直接切换
                if (buffer.isNextReady()) {
                    buffer.switchPos();
                    buffer.setNextReady(false);
                } else {
                    // 如果另一个Segment没有准备好，则返回异常双buffer全部用完
                    logger.error("Both two segments in {} are not ready!", buffer);
                    return new Result(EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL, Status.EXCEPTION);
                }
            } finally {
                // 释放写锁
                buffer.wLock().unlock();
            }
        }
    }

    /**
     * 自旋超时睡眠，如果自旋10000以内，线程池执行【准备Segment任务】结束就直接退出，否则就睡眠10ms，防止CPU空转
     * @param buffer buffer
     */
    private void waitAndSleep(SegmentBuffer buffer) {
        int roll = 0;
        while (buffer.getThreadRunning().get()) {
            roll += 1;
            if(roll > 10000) {
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                    break;
                } catch (InterruptedException e) {
                    logger.warn("Thread {} Interrupted",Thread.currentThread().getName());
                    break;
                }
            }
        }
    }

    public List<LeafAlloc> getAllLeafAllocs() {
        return dao.getAllLeafAllocs();
    }

    public Map<String, SegmentBuffer> getCache() {
        return cache;
    }

    public IDAllocDao getDao() {
        return dao;
    }

    public void setDao(IDAllocDao dao) {
        this.dao = dao;
    }
}
