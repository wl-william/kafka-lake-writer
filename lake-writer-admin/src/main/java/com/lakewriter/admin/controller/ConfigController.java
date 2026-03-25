package com.lakewriter.admin.controller;

import com.lakewriter.admin.service.ConfigService;
import com.lakewriter.admin.service.ConfigValidator;
import com.lakewriter.common.model.TopicSinkConfig;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST API for Topic Sink Config CRUD.
 *
 * GET    /api/v1/configs           - list (paginated, filter by status)
 * GET    /api/v1/configs/{id}      - get single
 * POST   /api/v1/configs           - create
 * PUT    /api/v1/configs/{id}      - update
 * DELETE /api/v1/configs/{id}      - delete
 * PUT    /api/v1/configs/{id}/pause
 * PUT    /api/v1/configs/{id}/resume
 * GET    /api/v1/configs/{id}/changelog
 * POST   /api/v1/configs/validate
 */
@RestController
@RequestMapping("/api/v1/configs")
public class ConfigController {

    private final ConfigService configService;
    private final ConfigValidator validator;

    public ConfigController(ConfigService configService, ConfigValidator validator) {
        this.configService = configService;
        this.validator = validator;
    }

    @GetMapping
    public ResponseEntity<?> list(@RequestParam(required = false) String status,
                                   @PageableDefault(size = 20) Pageable pageable) {
        Page<TopicSinkConfig> page = (status != null && !status.isEmpty())
            ? configService.findByStatus(status, pageable)
            : configService.findAll(pageable);
        return ok(page);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> get(@PathVariable Long id) {
        return ok(configService.findById(id));
    }

    @PostMapping
    public ResponseEntity<?> create(@RequestBody TopicSinkConfig config) {
        List<String> errors = validator.validate(config);
        if (!errors.isEmpty()) return badRequest(errors);
        TopicSinkConfig saved = configService.create(config);
        Map<String, Object> data = new HashMap<>();
        data.put("id", saved.getId());
        data.put("topicName", saved.getTopicName());
        data.put("status", saved.getStatus());
        data.put("version", saved.getVersion());
        data.put("message", "配置创建成功，将在30秒内生效");
        return ok(data);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> update(@PathVariable Long id, @RequestBody TopicSinkConfig config) {
        List<String> errors = validator.validate(config);
        if (!errors.isEmpty()) return badRequest(errors);
        return ok(configService.update(id, config));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable Long id) {
        configService.delete(id);
        return ok("deleted");
    }

    @PutMapping("/{id}/pause")
    public ResponseEntity<?> pause(@PathVariable Long id) {
        return ok(configService.pause(id));
    }

    @PutMapping("/{id}/resume")
    public ResponseEntity<?> resume(@PathVariable Long id) {
        return ok(configService.resume(id));
    }

    @GetMapping("/{id}/changelog")
    public ResponseEntity<?> changelog(@PathVariable Long id) {
        return ok(configService.getChangelog(id));
    }

    @PostMapping("/validate")
    public ResponseEntity<?> validate(@RequestBody Map<String, String> req) {
        String schemaJson = req.get("schemaJson");
        String sinkPath   = req.get("sinkPath");
        List<String> errors = validator.validateSchema(schemaJson);
        if (sinkPath != null && !validator.isValidPathTemplate(sinkPath)) {
            errors.add("sinkPath must start with / or oss:// or s3://");
        }
        Map<String, Object> result = new HashMap<>();
        result.put("valid", errors.isEmpty());
        result.put("errors", errors);
        return ok(result);
    }

    private ResponseEntity<?> ok(Object data) {
        Map<String, Object> r = new HashMap<>();
        r.put("code", 200);
        r.put("data", data);
        return ResponseEntity.ok(r);
    }

    private ResponseEntity<?> badRequest(Object errors) {
        Map<String, Object> r = new HashMap<>();
        r.put("code", 400);
        r.put("errors", errors);
        return ResponseEntity.badRequest().body(r);
    }
}
