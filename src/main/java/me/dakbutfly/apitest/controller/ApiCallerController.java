package me.dakbutfly.apitest.controller;

import com.google.common.collect.ImmutableMap;
import me.dakbutfly.apitest.entity.ApiCaller;
import me.dakbutfly.apitest.repository.ApiCallerRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
public class ApiCallerController {

    @Autowired
    private ApiCallerRepository apiCallerRepository;

    @RequestMapping(value = "/api/apicallers",method = RequestMethod.GET)
    public List<ApiCaller> list() {
        return Collections.<ApiCaller>emptyList();
    }

}
