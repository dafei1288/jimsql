package com.dafei1288.jimsql.ai;

import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class McpServerApplication {

    @Bean
    public ToolCallbackProvider sqlTools(JimsqlSqlService jimsqlSqlService ) {
        return MethodToolCallbackProvider.builder().toolObjects(jimsqlSqlService).build();
    }

    public static void main(String[] args) {
        boolean stdio = isStdio(args);
        SpringApplication app = new SpringApplication(McpServerApplication.class);
        if (stdio) {
            System.setProperty("org.springframework.boot.logging.LoggingSystem", "none");
            app.setBannerMode(org.springframework.boot.Banner.Mode.OFF);
            app.setLogStartupInfo(false);
            app.setWebApplicationType(WebApplicationType.NONE);
        }
        ConfigurableApplicationContext ctx = app.run(args);
        if (stdio) {
            try {
                StdioMcpRunner runner = ctx.getBean(StdioMcpRunner.class);
                runner.runLoop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean isStdio(String[] args) {        // Accept multiple switches: system properties (mcp.stdio, mcp_stdio, mcpstdio, mcpStdio),
        // environment (MCP_STDIO), and CLI flags (--mcp.stdio=true, --mcp_stdio=true, etc.)
        String[] keys = new String[] {"mcp.stdio", "mcp_stdio", "mcpstdio", "mcpStdio"};
        for (String k : keys) {
            String v = System.getProperty(k);
            if ("true".equalsIgnoreCase(v)) return true;
        }
        String env = System.getenv().getOrDefault("MCP_STDIO", "false");
        if ("true".equalsIgnoreCase(env)) return true;
        if (args != null) {
            for (String a : args) {
                String s = a == null ? "" : a.trim();
                if ("--mcp.stdio=true".equalsIgnoreCase(s) ||
                    "--mcp_stdio=true".equalsIgnoreCase(s) ||
                    "--mcpstdio=true".equalsIgnoreCase(s) ||
                    "--mcpStdio=true".equalsIgnoreCase(s) ||
                    "-mcp.stdio=true".equalsIgnoreCase(s) ||
                    "-mcp_stdio=true".equalsIgnoreCase(s) ||
                    "-mcpstdio=true".equalsIgnoreCase(s) ||
                    "-mcpStdio=true".equalsIgnoreCase(s)) {
                    return true;
                }
            }
        }
        return false;}
}


