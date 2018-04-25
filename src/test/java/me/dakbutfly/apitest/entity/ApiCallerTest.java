package me.dakbutfly.apitest.entity;

import me.dakbutfly.apitest.demo.MongConfig;
import me.dakbutfly.apitest.repository.ApiCallerRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.mongo.embedded.EmbeddedMongoAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.embedded.EmbeddedMongoProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = MongConfig.class)
@SpringBootTest
public class ApiCallerTest {

    @Autowired
    public ApiCallerRepository apiCallerRepository;

    @Test
    public void testIsOk() {
        List<ApiCaller> all = apiCallerRepository.findAll();

    }

}
