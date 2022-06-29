package com.sap.hellospark.controller;

import com.sap.hellospark.service.WordCountService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class WordCountController {

    private final WordCountService service;

    public WordCountController(WordCountService service) {
        this.service = service;
    }

    @PostMapping(path = "/wordcount")
    public Map<Integer, Long> count(@RequestParam String numbers) {
        List<Integer> input = Arrays.stream(numbers.split("\\|")).map(Integer::parseInt).collect(Collectors.toList());
        return service.getFactorials(input);
    }
}
