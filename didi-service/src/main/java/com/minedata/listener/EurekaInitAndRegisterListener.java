package com.minedata.listener;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;
import java.util.Date;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.appinfo.MyDataCenterInstanceConfig;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.discovery.DefaultEurekaClientConfig;
import com.netflix.discovery.DiscoveryManager;

// @WebListener
public class EurekaInitAndRegisterListener implements ServletContextListener {

    private static final DynamicPropertyFactory configInstance =
            com.netflix.config.DynamicPropertyFactory.getInstance();

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
        DiscoveryManager.getInstance().shutdownComponent();
    }

    public void registerWithEureka() {
        /** 加载本地配置文件 根据配置初始化这台 Eureka Application Service 并且注册到 Eureka Server */
        DiscoveryManager.getInstance().initComponent(new MyDataCenterInstanceConfig(),
                new DefaultEurekaClientConfig());
        ApplicationInfoManager.getInstance().setInstanceStatus(InstanceStatus.UP);
        String vipAddress =
                configInstance.getStringProperty("eureka.vipAddress", "localhost").get();
        InstanceInfo nextServerInfo = null;
        while (nextServerInfo == null) {
            try {
                nextServerInfo =
                        DiscoveryManager.getInstance().getDiscoveryClient()
                                .getNextServerFromEureka(vipAddress, false);
            } catch (Throwable e) {
                System.out.println("Waiting for service to register with eureka..");


                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }


            }
        }
        System.out.println("Service started and ready to process requests..");
        //
        //
        // try {
        // ServerSocket serverSocket =
        // new ServerSocket(configInstance.getIntProperty("eureka.port", 8010).get());
        // final Socket s = serverSocket.accept();
        // System.out.println("Client got connected..Processing request from the client");
        // processRequest(s);
        //
        //
        // } catch (IOException e) {
        // e.printStackTrace();
        // }

        /** Application Service 的 Eureka Server 初始化以及注册是异步的，需要一段时间 此处等待初始化及注册成功 可去除 */
        // String vipAddress = configInstance.getStringProperty("eureka.vipAddress", "o2o").get();
        // InstanceInfo nextServerInfo = null;
        // while (nextServerInfo == null) {
        // try {
        // nextServerInfo = eurekaClient.getNextServerFromEureka(vipAddress, false);
        // } catch (Throwable e) {
        // try {
        // Thread.sleep(10000);
        // } catch (InterruptedException e1) {
        // e1.printStackTrace();
        // }
        //
        //
        // }
        // }
    }

    private void processRequest(final Socket s) {
        try {
            BufferedReader rd = new BufferedReader(new InputStreamReader(s.getInputStream()));
            String line = rd.readLine();
            if (line != null) {
                System.out.println("Received the request from the client.");
            }
            PrintStream out = new PrintStream(s.getOutputStream());
            System.out.println("Sending the response to the client...");


            out.println("Reponse at " + new Date());


        } catch (Throwable e) {
            System.err.println("Error processing requests");
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }


    }

    @Override
    public void contextInitialized(ServletContextEvent arg0) {
        /** 设置被读取配置文件名称 默认config.properties */
        // PROPERTIES PROPERTIES = NEW PROPERTIES();
        // PROPERTIES.SETPROPERTY("ARCHAIUS.CONFIGURATIONSOURCE.DEFAULTFILENAME",
        // "CONFIG.PROPERTIES");
        // SYSTEM.SETPROPERTIES(PROPERTIES);
        /** 注册 */
        registerWithEureka();
    }

}
