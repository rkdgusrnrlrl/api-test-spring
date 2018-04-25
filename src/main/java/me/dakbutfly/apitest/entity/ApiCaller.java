package me.dakbutfly.apitest.entity;

import org.springframework.data.annotation.Id;

public class ApiCaller {

    @Id
    public Long id;

    public String baseUrl;
    public String apiUrl;
    public String method;
    public String body;
    public String queryString;

    public ApiCaller(Long id, String baseUrl, String apiUrl, String method, String body, String queryString) {
        this.id = id;
        this.baseUrl = baseUrl;
        this.apiUrl = apiUrl;
        this.method = method;
        this.body = body;
        this.queryString = queryString;
    }
}
