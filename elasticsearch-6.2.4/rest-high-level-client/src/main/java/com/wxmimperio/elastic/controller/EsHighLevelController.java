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

    @PostMapping("createIndexAsync/{indexName}")
    public void createIndexAsync(@PathVariable String indexName) {
        service.createIndexAsync(indexName);
    }

    @PostMapping("createIndexSync/{indexName}")
    public void createIndexSync(@PathVariable String indexName) {
        service.createIndexSync(indexName);
    }

    @DeleteMapping("deleteIndexAsync/{indexName}")
    public void deleteIndexAsync(@PathVariable String indexName) {
        service.deleteIndexAsync(indexName.split(",", -1));
    }

    @DeleteMapping("deleteIndexSync/{indexName}")
    public void deleteIndexSync(@PathVariable String indexName) {
        service.deleteIndexSync(indexName.split(",", -1));
    }

    @PutMapping("closeIndexSync/{indexName}")
    public void closeIndexSync(@PathVariable String indexName) {
        service.closeIndexSync(indexName.split(",", -1));
    }

    @PutMapping("closeIndexAsync/{indexName}")
    public void closeIndexAsync(@PathVariable String indexName) {
        service.closeIndexAsync(indexName.split(",", -1));
    }

    @PutMapping("openIndexSync/{indexName}")
    public void openIndexSync(@PathVariable String indexName) {
        service.openIndexSync(indexName.split(",", -1));
    }

    @PutMapping("openIndexAsync/{indexName}")
    public void openIndexAsync(@PathVariable String indexName) {
        service.openIndexAsync(indexName.split(",", -1));
    }
}
