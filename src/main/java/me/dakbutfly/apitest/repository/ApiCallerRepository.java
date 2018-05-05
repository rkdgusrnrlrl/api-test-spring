package me.dakbutfly.apitest.repository;

import me.dakbutfly.apitest.entity.ApiCaller;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

import java.util.List;

@RepositoryRestResource(collectionResourceRel = "apicallers", path = "apicallers")
public interface ApiCallerRepository extends MongoRepository<ApiCaller, Long> {

    public ApiCaller findApiCallerById(Long id);
    public List<ApiCaller> findAll();

}
