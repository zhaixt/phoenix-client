package com.science;

import com.science.Exception.UniqueKeyException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by zhaixiaotong on 2017-1-13.
 */
@Slf4j
public class UniqueKey {

    private static volatile boolean isInit = false;
    Connection connection;
    Admin admin;

    private final Lock lock = new ReentrantLock();


    //    private static final String DEFAULT_UNIQUE_KEY_TABLE_NAME = "sequence_info";
    private static final String DEFAULT_COLUMN_FAMILY = "info";
    private static final String DEFAULT_COLUMN_NAME = "value";

    private static final int RETRY_TIMES = 3;

    public void init() throws IOException, Exception {
        if (!isInit) {
            synchronized (UniqueKey.class) {
                if (!isInit) {


                    Configuration conf = HBaseConfiguration.create();
                    //        conf.set("hbase.master", "master:60000");
                    conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
                    conf.set("hbase.zookeeper.property.clientport", "2181");

                    UserGroupInformation.setConfiguration(conf);

                    connection = ConnectionFactory.createConnection(conf);
                    admin = connection.getAdmin();

                }
            }
        }

    }

    public boolean checkUniqueKey(String tableName, String uniqueKey) throws UniqueKeyException, IOException {
        boolean isExists = isExists(tableName);
        if (!isExists) {
            createTable(tableName);
        }
        log.info("increase data from table " + tableName + " .");
        Table table = connection.getTable(TableName.valueOf(tableName));
        byte[] family = Bytes.toBytes(DEFAULT_COLUMN_FAMILY);
//        byte[] row = Bytes.toBytes("baidu.com_19991011_20151011");
        byte[] row = Bytes.toBytes(uniqueKey);
        Get get = new Get(row);
        get.addFamily(family);

        get.addColumn(family, Bytes.toBytes(DEFAULT_COLUMN_NAME));
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        if (cells == null) {
            putUniqueKey(table, uniqueKey);
            return true;
        } else if (cells.size() == 1) {
            return false;
        } else {
            throw new UniqueKeyException("the unique key number:" + uniqueKey + " in table: " + tableName + " exceeds 1");
        }

    }

    private void putUniqueKey(Table table, String uniqueKey) throws IOException {
        byte[] rowkey = Bytes.toBytes(uniqueKey);
        Put put = new Put(rowkey);
        byte[] family = Bytes.toBytes(DEFAULT_COLUMN_FAMILY);
        byte[] qualifier = Bytes.toBytes(DEFAULT_COLUMN_NAME);
        byte[] value = Bytes.toBytes("default");
        put.addColumn(family, qualifier, value);
        table.put(put);
    }

    /**
     * 判断表是否存在
     *
     * @param tableNameString
     * @return
     * @throws IOException
     */
    private boolean isExists(String tableNameString) throws IOException {
        TableName tableName = TableName.valueOf(tableNameString);

        boolean exists = admin.tableExists(tableName);
        if (exists) {
            log.info("Table " + tableName.getNameAsString() + " already exists.");
            System.out.println("Table " + tableName.getNameAsString() + " already exists.");
        } else {
            log.info("Table " + tableName.getNameAsString() + " not exists.");
            System.out.println("Table " + tableName.getNameAsString() + " not exists.");
        }
        return exists;
    }

    /**
     * 创建表
     *
     * @param tableNameString
     * @throws IOException
     */
    private void createTable(String tableNameString) throws IOException {
        TableName tableName = TableName.valueOf(tableNameString);
        log.info("To create table named " + tableNameString);
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        HColumnDescriptor columnDesc = new HColumnDescriptor(DEFAULT_COLUMN_FAMILY);//列族
        tableDesc.addFamily(columnDesc);

        admin.createTable(tableDesc);
    }
}
