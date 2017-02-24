package com.science;

import com.science.sequence.PhoenixSequence;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by zhaixiaotong on 2017-1-12.
 */
@Slf4j
public class UniqueKeyTest {
    @Before
    public void setUp() throws Exception {

    }

    @After
    public void close(){
    }
    @Test
    public void test() throws Exception {
//        assertTrue();
//        assertArrayEquals("123".getBytes(), "123".getBytes());
//        assertEquals("abcdefg", new String("")));
    }
    /*
    *  单机环境，只init一次，单线程循环获取sequence，只是用来验证功能
     */
    @Test
    public void singleThreadTest() throws Exception {
        final UniqueKey uniqueKey = new UniqueKey();
        try {
            uniqueKey.init();
            boolean seq;
            for(int i = 0;i<5;i++){
                 seq = uniqueKey.checkUniqueKey("ZHAIXT_UNIQUEKEY","uniqueKey1");
                System.out.println("now the table uniquekey result is :" + seq+",count:"+i);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /*
    *  单机环境，只init一次，但是多个线程get sequence，模拟单机情况下web应用在多线程情况下的取值
    * */
    @Test
    public void multiThreadTest() throws Exception {
        final PhoenixSequence sequence = new PhoenixSequence();
        try {
            sequence.init();//
//
        } catch (Exception e) {
            e.printStackTrace();
        }
        ExecutorService executorService = Executors.newCachedThreadPool();
        for(int i =0;i<10;i++){
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    String threadName = Thread.currentThread().getName();
                    try {


                        long seq;
                        for(int i = 0;i<120;i++){
                            seq = sequence.getSequenceNum("zhaixt");
                            System.out.println("Thread:"+threadName+",now the table zhaixt sequence is :" + seq+",count:"+i);

                        }

                        seq = sequence.getSequenceNum("zhaixt_test");
                        System.out.println("Thread:"+threadName+",now the table zhaixt_test sequence is :"+seq);
                    } catch (Exception e) {
                        log.error("init error", e);
                    } finally {

                    }
                }
            });

        }

    }

     /*
      * 多实例模式，切有多个线程get sequence，模拟分布式集群情况下web应用在多线程情况下的取值
      * 多个实例多个线程不好测，因为isInit是static的变量
     */
    @Test
    public void multiInstanceTest() throws Exception {

        ExecutorService executorService = Executors.newCachedThreadPool();
        for(int i =0;i<10;i++){
            executorService.execute(new Runnable() {
                @Override
                public void run() {

                    try {

                        final PhoenixSequence sequence = new PhoenixSequence();
                        sequence.init();//
                        String threadName = Thread.currentThread().getName();
                        long seq;
                        for(int i = 0;i<240;i++){
                            seq = sequence.getSequenceNum("zhaixt");
                            System.out.println("Thread:"+threadName+",now the table zhaixt sequence is :" + seq+",count:"+i);

                        }

                        seq = sequence.getSequenceNum("zhaixt_test");
                        System.out.println("Thread:"+threadName+",now the table zhaixt_test sequence is :"+seq);
                    } catch (Exception e) {
                        log.error("init error", e);
                    } finally {

                    }
                }
            });

            executorService.shutdown();
        }
    }
}
