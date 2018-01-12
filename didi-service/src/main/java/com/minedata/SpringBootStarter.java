package com.minedata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * 使用SpringBoot启动容器
 */
@SpringBootApplication
@ComponentScan
@EnableAsync
@ServletComponentScan
@ImportResource(locations = {"META-INF/spring/application-dubbo-provider.xml"})
public class SpringBootStarter {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootStarter.class, args);
    }
}
