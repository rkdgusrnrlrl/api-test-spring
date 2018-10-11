package me.dakbutfly.apitest.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import me.dakbutfly.apitest.demo.DemoApplication;
import me.dakbutfly.apitest.entity.ApiCaller;
import me.dakbutfly.apitest.repository.ApiCallerRepository;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.*;
import org.springframework.boot.test.autoconfigure.web.servlet.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MockMvcBuilder;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.BDDMockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.assertj.core.api.Assertions.*;

public class TestApiCallerController {


    private MockMvc mockMvc;

    @Mock
    private ApiCallerRepository apiCallerRepository;

    @InjectMocks
    private ApiCallerController apiCallerController;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(apiCallerController).build();
    }

    @Test
    public void testGetApiCallerList() throws Exception {
        mockMvc.perform(get("/api/apicallers"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    public void whenCreateApiCallerShouldBeExistOneApiCaller() throws Exception {
        ApiCaller apiCaller = ApiCaller.builder()
                .baseUrl("http://api.dakbutfly.me")
                .apiUrl("/hello")
                .method("POST")
                .build();

        when(apiCallerRepository.save(apiCaller)).thenReturn(apiCaller);


        mockMvc.perform(post("/api/apicallers").content(asJsonString(apiCaller)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("ok").value(true));

        verify(apiCallerRepository, times(1)).save(refEq(apiCaller));
    }

    static String asJsonString(final Object obj) {
        try {
            return new ObjectMapper().writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
