package com.hazelnut;

import com.hazelnut.node.NodeStartup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PostConstruct;

@SpringBootApplication
@EnableScheduling
@ComponentScan(basePackages = "com.hazelnut")
public class HazelNutApplication {

    public static final String NODE_ID = java.util.UUID.randomUUID().toString();

    public static void main(String[] args) {
        SpringApplication.run(HazelNutApplication.class, args);
    }
    @Autowired
    NodeStartup service;

    @PostConstruct
    public void init(){
        service.bootStrapNodeAndCluster();
    }

}
