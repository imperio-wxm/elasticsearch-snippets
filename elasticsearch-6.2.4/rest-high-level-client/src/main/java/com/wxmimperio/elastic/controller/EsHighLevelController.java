package com.wxmimperio.elastic.controller;

import com.wxmimperio.elastic.service.EsHighLevelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("esHighLevel")
public class EsHighLevelController {

    private EsHighLevelService service;

    @Autowired
    public EsHighLevelController(EsHighLevelService service) {
        this.service = service;
    }

    @GetMapping("createIndexAsync/{indexName}")
    public void createIndexAsync(@PathVariable String indexName) {
        service.createIndexAsync(indexName);
    }

    @DeleteMapping("deleteIndexAsync/{indexName}")
    public void deleteIndexAsync(@PathVariable String indexName) {
        service.deleteIndexAsync(indexName.split(",", -1));
    }
}
