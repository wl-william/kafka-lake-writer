package com.lakewriter.worker.config;

import com.lakewriter.common.model.TopicSinkConfig;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Computes the difference between two config snapshots.
 * Used by ConfigPoller to determine what actions to take when configs change.
 */
@Getter
public class ConfigDiff {

    private final List<TopicSinkConfig> added;
    private final List<TopicSinkConfig> removed;
    private final List<TopicSinkConfig> schemaChanged;   // schema_json changed
    private final List<TopicSinkConfig> otherChanged;    // path/format/flush changed

    public ConfigDiff(List<TopicSinkConfig> added, List<TopicSinkConfig> removed,
                      List<TopicSinkConfig> schemaChanged, List<TopicSinkConfig> otherChanged) {
        this.added = added;
        this.removed = removed;
        this.schemaChanged = schemaChanged;
        this.otherChanged = otherChanged;
    }

    public boolean isEmpty() {
        return added.isEmpty() && removed.isEmpty() && schemaChanged.isEmpty() && otherChanged.isEmpty();
    }

    /**
     * Compare old and new config maps and return a ConfigDiff.
     * Detection uses the version field — any version change is a "change".
     */
    public static ConfigDiff compute(Map<String, TopicSinkConfig> oldMap,
                                     Map<String, TopicSinkConfig> newMap) {
        List<TopicSinkConfig> added = new ArrayList<>();
        List<TopicSinkConfig> removed = new ArrayList<>();
        List<TopicSinkConfig> schemaChanged = new ArrayList<>();
        List<TopicSinkConfig> otherChanged = new ArrayList<>();

        // New configs not in old
        for (Map.Entry<String, TopicSinkConfig> e : newMap.entrySet()) {
            if (!oldMap.containsKey(e.getKey())) {
                added.add(e.getValue());
            }
        }

        // Old configs not in new
        for (Map.Entry<String, TopicSinkConfig> e : oldMap.entrySet()) {
            if (!newMap.containsKey(e.getKey())) {
                removed.add(e.getValue());
            }
        }

        // Changed configs — detect by version number
        for (Map.Entry<String, TopicSinkConfig> e : newMap.entrySet()) {
            TopicSinkConfig oldCfg = oldMap.get(e.getKey());
            TopicSinkConfig newCfg = e.getValue();
            if (oldCfg != null && oldCfg.getVersion() != newCfg.getVersion()) {
                // Classify: schema change requires flush + buffer rebuild
                if (!safeEquals(oldCfg.getSchemaJson(), newCfg.getSchemaJson())) {
                    schemaChanged.add(newCfg);
                } else {
                    otherChanged.add(newCfg);
                }
            }
        }

        return new ConfigDiff(added, removed, schemaChanged, otherChanged);
    }

    private static boolean safeEquals(String a, String b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }
}
