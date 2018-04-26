package me.dakbutfly.apitest.entity;

import lombok.Builder;
import org.springframework.data.annotation.Id;

@Builder
public class ApiCaller {

    @Id
    public String id;

    public String baseUrl;
    public String apiUrl;
    public String method;
    public String body;
    public String queryString;
}
