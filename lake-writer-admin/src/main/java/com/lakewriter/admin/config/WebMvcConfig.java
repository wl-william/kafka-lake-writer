package com.lakewriter.admin.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * SPA (Single Page Application) fallback routing.
 * All non-API, non-static routes forward to index.html
 * so Vue Router can handle client-side navigation.
 */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/**")
                .addResourceLocations("classpath:/static/");
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        // Forward SPA routes to index.html
        registry.addViewController("/").setViewName("forward:/index.html");
        registry.addViewController("/configs").setViewName("forward:/index.html");
        registry.addViewController("/configs/**").setViewName("forward:/index.html");
        registry.addViewController("/monitor").setViewName("forward:/index.html");
        registry.addViewController("/monitor/**").setViewName("forward:/index.html");
        registry.addViewController("/nodes").setViewName("forward:/index.html");
        registry.addViewController("/changelog").setViewName("forward:/index.html");
        registry.addViewController("/changelog/**").setViewName("forward:/index.html");
    }
}
