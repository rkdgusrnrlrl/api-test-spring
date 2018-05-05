package me.dakbutfly.apitest.demo;

import me.dakbutfly.apitest.repository.ApiCallerRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@ComponentScan(basePackages = "me.dakbutfly.apitest")
@EnableMongoRepositories(basePackages = "me.dakbutfly.apitest.repository")
@EnableAutoConfiguration
@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
