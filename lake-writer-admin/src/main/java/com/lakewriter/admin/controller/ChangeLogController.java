package com.lakewriter.admin.controller;

import com.lakewriter.admin.service.ConfigService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Changelog endpoint (also available via /api/v1/configs/{id}/changelog,
 * this provides a top-level entry point).
 */
@RestController
@RequestMapping("/api/v1/changelog")
public class ChangeLogController {

    private final ConfigService configService;

    public ChangeLogController(ConfigService configService) {
        this.configService = configService;
    }

    @GetMapping("/config/{configId}")
    public ResponseEntity<?> byConfig(@PathVariable Long configId) {
        Map<String, Object> r = new HashMap<>();
        r.put("code", 200);
        r.put("data", configService.getChangelog(configId));
        return ResponseEntity.ok(r);
    }
}
