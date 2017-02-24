package com.science.sequence;

import com.science.Exception.SequenceException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by zhaixiaotong on 2017-1-10.
 */
@Slf4j
public class PhoenixSequence {
    private static volatile boolean isInit = false;
    Connection connection;
    Admin admin;
//    private   Map<String,AtomicInteger> countMap;//计数器
    private  Map<String,CountInfo> countMap ;//计数器
    public  Map<String,AtomicLong> sequenceInfoMap;
    private final Lock lock = new ReentrantLock();

    private static final int DEFAULT_INNER_STEP = 1000;
    private static final String DEFAULT_SEQUENCE_TABLE_NAME = "sequence_info";
    private static final String DEFAULT_COLUMN_FAMILY = "info";
    private static final String DEFAULT_VALUE_COLUMN_NAME = "sequence";
    private static final int RETRY_TIMES = 3;

    public void init() throws IOException,Exception{
        if(!isInit){
            synchronized (PhoenixSequence.class){
                if(!isInit){
                    Configuration conf = HBaseConfiguration.create();
                    //        conf.set("hbase.master", "master:60000");
                    conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
                    conf.set("hbase.zookeeper.property.clientport", "2181");
                    UserGroupInformation.setConfiguration(conf);

                    connection = ConnectionFactory.createConnection(conf);
                    admin = connection.getAdmin();

                    this.countMap = new HashMap<>();
                    this.sequenceInfoMap =  new HashMap<>();
                    //获取初始的sequence 信息map
                    sequenceInfoMap = getInitialSequenceMap(connection);

                }
            }
        }

    }
    //获取初始的sequence 信息map
    private Map<String,AtomicLong> getInitialSequenceMap(Connection connection) throws IOException{
        Map<String,AtomicLong> sequenceMap = new HashMap<>();
        List<String> rowKeyList = new ArrayList<>();
//        Map<String,AtomicInteger> countMap = new HashMap<>();
        log.info("Scan table " + DEFAULT_SEQUENCE_TABLE_NAME + " to browse all datas.");
        System.out.println("-----Scan table " + DEFAULT_SEQUENCE_TABLE_NAME + " to browse all datas.-----");
        TableName tableName = TableName.valueOf(DEFAULT_SEQUENCE_TABLE_NAME);
        byte[] family = Bytes.toBytes(DEFAULT_COLUMN_FAMILY);

        Scan scan = new Scan();
        scan.addFamily(family);

        Table table = connection.getTable(tableName);
        ResultScanner resultScanner = table.getScanner(scan);
        for(Iterator<Result> it = resultScanner.iterator(); it.hasNext();) {
            Result result = it.next();
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String qualifier = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell), "UTF-8");
                String rowKey = new String(CellUtil.cloneRow(cell),"UTF-8");
                rowKeyList.add(rowKey);

                // @Deprecated
                // LOG.info(cell.getQualifier() + "\t" + cell.getValue());
                log.info("rowkey:"+rowKey +",qualifier:"+qualifier + ",value:" + value);
                System.out.println(rowKey + " " + qualifier + "  " + value);
            }
        }
        for(int i = 0; i<rowKeyList.size();i++){
            String rowKey = rowKeyList.get(i);
            Long value = table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes(DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DEFAULT_VALUE_COLUMN_NAME), DEFAULT_INNER_STEP);
            System.out.println("Thread:"+Thread.currentThread().getName()+",table:"+rowKey+",get initial sequence value is:"+value);
            sequenceMap.put(rowKey,new AtomicLong(value));
            //初始化计数器
            countMap.put(rowKey,new CountInfo(new AtomicInteger(0),true));
        }
        System.out.println("-----Scan table " + DEFAULT_SEQUENCE_TABLE_NAME + " OVER-----");
        return sequenceMap;
    }

    //需要重新拉取一次sequence，并将sequence表中加一下步长。
    private Long  adjustSequenceInfo(Connection connection,String rowKey) throws IOException{

//        synchronized()
        log.info("increase data from table " + DEFAULT_SEQUENCE_TABLE_NAME + " .");
        Table table = connection.getTable(TableName.valueOf(DEFAULT_SEQUENCE_TABLE_NAME));

        long count = table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes(DEFAULT_COLUMN_FAMILY), Bytes.toBytes(DEFAULT_VALUE_COLUMN_NAME), DEFAULT_INNER_STEP);
        //todo 是否需要减去步长step

        sequenceInfoMap.get(rowKey).set(count);
        log.info("-------incresed data is: " + count);
        System.out.println("-------incresed data is: " + count);
        return count;


    }
    public long getSequenceNum(String table) throws IOException,SequenceException {
        long currentSequenceNum;
//        int count;
//        this.lock.lock();
//        private  volatile boolean isInit = false;
        if(sequenceInfoMap.get(table) != null){
            //
            if(countMap.get(table).isStatus()) {
                if (countMap.get(table).getCount().get() > DEFAULT_INNER_STEP - 1) {

                    countMap.get(table).getCount().set(0);
                    //此时不允许在获取sequence number
                    countMap.get(table).setStatus(false);
                    //todo 想明白这块
                    //  synchronized (sequenceInfoMap.get(table)){//obj
//                    synchronized (this) {//obj
                    System.out.println("increase the sequence by inner step from table:"+table);
                    currentSequenceNum = adjustSequenceInfo(connection, table);
                    countMap.get(table).setStatus(true);

                } else {
                    currentSequenceNum = sequenceInfoMap.get(table).getAndIncrement();
                    countMap.get(table).getCount().getAndIncrement();
                }
            }else{
                //如果试了三次后，还是不成功，那么返回-1
                currentSequenceNum = -1;
                for(int i = 0;i<RETRY_TIMES;i++){
                    try {
                        Thread.sleep(500);
                        if(countMap.get(table).isStatus()) {
                            currentSequenceNum = sequenceInfoMap.get(table).getAndIncrement();
                            countMap.get(table).getCount().getAndIncrement();
                            break;
                        }
                    } catch (InterruptedException e) {
                        log.error("retry to get the  currentSequenceNum error",e);
                        e.printStackTrace();
                    }
                }

            }


        }else{
            throw new SequenceException("can not find the table from sequence info");
        }
        return currentSequenceNum;

    }


    public static void main(String[] args) {
        /*
        * 多线程
        * */
        final PhoenixSequence sequence = new PhoenixSequence();
        try {
            sequence.init();//
//
        } catch (Exception e) {
            e.printStackTrace();
        }
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < 10; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    String threadName = Thread.currentThread().getName();
                    try {


                        long seq;
                        for (int i = 0; i < 120; i++) {
                            seq = sequence.getSequenceNum("zhaixt");
                            System.out.println("Thread:" + threadName + ",now the table zhaixt sequence is :" + seq + ",count:" + i);

                        }

                        seq = sequence.getSequenceNum("zhaixt_test");
                        System.out.println("Thread:" + threadName + ",now the table zhaixt_test sequence is :" + seq);
                    } catch (Exception e) {
                        log.error("init error", e);
                    } finally {

                    }
                }
            });

        }
    }
}
