package me.dakbutfly.apitest.controller;

import me.dakbutfly.apitest.demo.DemoApplication;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.boot.test.autoconfigure.web.servlet.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.ResultMatcher;

import static org.mockito.BDDMockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.assertj.core.api.Assertions.*;

@RunWith(SpringRunner.class)
@WebMvcTest(ApiCallerController.class)
@ContextConfiguration(classes = {DemoApplication.class})
public class TestApiCallerController {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void testGetApiCallerList() throws Exception {
        mockMvc.perform(get("/api/apicallers"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("apiCallerList.length()").value(0));
    }
}
