package com.science.util;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.util.InstanceResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhaixiaotong on 2017-2-24.
 */
public class ScienceUtil {

    private static final Logger log = LoggerFactory.getLogger(ScienceUtil.class);
    private static volatile boolean isInit = false;

    public ScienceUtil() {
    }

    public static void init() {
        if (!isInit) {
            Class scienceUtilClass = ScienceUtil.class;
            synchronized (ScienceUtil.class) {
                if (!isInit) {

                    final Configuration conf = HBaseConfiguration.create();
                    conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
                    conf.set("hbase.zookeeper.property.clientport", "2181");
                    try {
                        UserGroupInformation.setConfiguration(conf);
                        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
                            public Configuration getConfiguration() {
                                return conf;
                            }

                            public Configuration getConfiguration(Configuration confToClone) {
                                Configuration copy = new Configuration(conf);
                                copy.addResource(confToClone);
                                return copy;
                            }
                        });
                        isInit = true;
                    } catch (Exception var7) {
                        log.error("init kerberos error", var7);
                    }
                }
            }

        }

    }
}
