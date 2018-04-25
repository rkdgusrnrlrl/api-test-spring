package me.dakbutfly.apitest.repository;

import me.dakbutfly.apitest.entity.ApiCaller;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface ApiCallerRepository extends MongoRepository<ApiCaller, Long> {

    public ApiCaller findApiCallerById(Long id);
    public List<ApiCaller> findAll();

}
