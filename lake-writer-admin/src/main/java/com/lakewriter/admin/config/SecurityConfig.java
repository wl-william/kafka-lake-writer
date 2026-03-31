package com.lakewriter.admin.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Spring Security configuration for the Admin module.
 *
 * - /api/** requires authentication (HTTP Basic)
 * - Static resources (Vue.js SPA) are accessible without auth
 * - Actuator health/info endpoints are open
 * - CSRF disabled for REST API (stateless, no browser form submissions)
 *
 * Credentials are supplied via environment variables:
 *   ADMIN_USERNAME (default: admin)
 *   ADMIN_PASSWORD (required — no default for security)
 */
@Configuration
public class SecurityConfig {

    @Value("${lake-writer.admin.username:admin}")
    private String adminUsername;

    @Value("${lake-writer.admin.password:}")
    private String adminPassword;

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .csrf().disable()
            .authorizeRequests()
                // Actuator health/info — open for monitoring
                .antMatchers("/actuator/health", "/actuator/info").permitAll()
                // API endpoints — require authentication
                .antMatchers("/api/**").authenticated()
                // Everything else (static resources, SPA routes) — open
                .anyRequest().permitAll()
            .and()
            .httpBasic();
        return http.build();
    }

    @Bean
    public UserDetailsService userDetailsService(PasswordEncoder encoder) {
        String password = adminPassword;
        if (password == null || password.isEmpty()) {
            password = "changeme";
        }
        return new InMemoryUserDetailsManager(
            User.builder()
                .username(adminUsername)
                .password(encoder.encode(password))
                .roles("ADMIN")
                .build()
        );
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }
}
