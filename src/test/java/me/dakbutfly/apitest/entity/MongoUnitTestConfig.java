package me.dakbutfly.apitest.entity;

import com.github.fakemongo.Fongo;
import com.mongodb.MongoClient;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@Configuration
@EnableMongoRepositories(basePackages = "me.dakbutfly.apitest.repository")
public class MongoUnitTestConfig extends AbstractMongoConfiguration {
    @Override
    public MongoClient mongoClient() {
        Fongo agenda = new Fongo("agenda");
        return agenda.getMongo();
    }

    @Override
    protected String getDatabaseName() {
        return "api";
    }
}
