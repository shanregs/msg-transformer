package com.bmo.sftr.mapping;

import com.bmo.sftr.mapping.model.SftrConstants;
import java.io.IOException;
import java.util.List;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** Shared test helpers. Constants come from the real application.yml, never hand-written maps. */
public final class TestSupport {
    private TestSupport() {}

    public static final ObjectMapper MAPPER = JsonMapper.builder().build();

    public static SftrConstants realConstants() {
        return bindConstants("application.yml");
    }

    public static SftrConstants bindConstants(String yamlOnClasspath) {
        try {
            List<PropertySource<?>> sources =
                    new YamlPropertySourceLoader().load("yaml", new ClassPathResource(yamlOnClasspath));
            MutablePropertySources mps = new MutablePropertySources();
            sources.forEach(mps::addLast);
            return new Binder(ConfigurationPropertySources.from(mps))
                    .bind("sftr.constants", Bindable.of(SftrConstants.class))
                    .get();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
