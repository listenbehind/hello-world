package com.minedata.service.rticHistory;

import org.apache.hadoop.conf.Configuration;

import com.minedata.service.didi.DidiRestServiceImpl;

public class RTICProperties {
    private static Configuration prop = new Configuration();
    public final static String lzop_path;
    public final static String rtic_home;
    public final static String redis_host;
    public final static int redis_port;
    public final static String crsPdPath;
    static {
        // prop.addResource(new FileInputStream(new File("./env_config.xml")));
        prop.addResource(
                DidiRestServiceImpl.class.getClassLoader().getResourceAsStream("./env_config.xml"),
                "utf-8");
        lzop_path = prop.get("lzop-path");
        rtic_home = prop.get("rtic-home");
        redis_host = prop.get("redis-host");
        redis_port = prop.getInt("redis-port", 6380);
        crsPdPath = prop.get("crsPdPath");
    }
}
