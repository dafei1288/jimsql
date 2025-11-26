package com.dafei1288.jimsql.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Locale;

/**
 * Minimal HTTP JSON-RPC endpoint to mirror the stdio MCP behavior.
 * POST / with a JSON-RPC 2.0 request; returns a single JSON-RPC response.
 */
@RestController
public class HttpMcpController {

    private final JimsqlSqlService service;
    private final ObjectMapper mapper = new ObjectMapper();

    public HttpMcpController(JimsqlSqlService service) {
        this.service = service;
    }
    @GetMapping("/ping")
    public String ping() {
        return "ok";
    }

    @PostMapping(value = "/", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ObjectNode> handle(@RequestBody JsonNode req) {
        ObjectNode resp = mapper.createObjectNode();
        resp.put("jsonrpc", "2.0");
        JsonNode id = req.get("id");
        if (id != null) resp.set("id", id);
        String method = req.has("method") ? req.get("method").asText("") : "";
        JsonNode params = req.get("params");
        try {
            switch (method) {
                case "initialize": {
                    ObjectNode result = mapper.createObjectNode();
                    result.put("protocolVersion", "2024-11-05");
                    ObjectNode caps = mapper.createObjectNode();
                    ObjectNode tools = mapper.createObjectNode();
                    tools.put("listChanged", true);
                    caps.set("tools", tools);
                    result.set("capabilities", caps);
                    ObjectNode si = mapper.createObjectNode();
                    si.put("name", "jimsql-mcp");
                    si.put("version", "3.3.9");
                    result.set("serverInfo", si);
                    resp.set("result", result);
                    break;
                }
                case "tools/list": {
                    ObjectNode result = mapper.createObjectNode();
                    ArrayNode tools = mapper.createArrayNode();
                    tools.add(tool(
                            "executeSql",
                            "Execute SQL and return a JSON array. For SELECT, append LIMIT 100 if missing.",
                            schema(new String[]{"sql"})
                    ));
                    tools.add(tool(
                            "nlQuery",
                            "Natural language query (Chinese/English). You can also pass raw SQL; returns JSON.",
                            schema(new String[]{"question"})
                    ));
                    tools.add(tool(
                            "getSchema",
                            "Get database schema (tables and columns).",
                            emptySchema()
                    ));
                    result.set("tools", tools);
                    resp.set("result", result);
                    break;
                }
                case "tools/call": {
                    String name = params != null && params.has("name") ? params.get("name").asText("") : "";
                    JsonNode args = params != null ? params.get("arguments") : null;
                    String text;
                    switch (name) {
                        case "executeSql":
                            text = service.executeSql(nonNullText(args, "sql"));
                            break;
                        case "nlQuery":
                            text = service.nlQuery(nonNullText(args, "question"));
                            break;
                        case "getSchema":
                            text = service.getSchema();
                            break;
                        default:
                            return ResponseEntity.ok(error(resp, -32601, "Tool not found: " + name));
                    }
                    ObjectNode result = mapper.createObjectNode();
                    ArrayNode content = mapper.createArrayNode();
                    ObjectNode textPart = mapper.createObjectNode();
                    textPart.put("type", "text");
                    textPart.put("text", text);
                    content.add(textPart);
                    result.set("content", content);
                    resp.set("result", result);
                    break;
                }
                case "shutdown":
                case "exit": {
                    ObjectNode result = mapper.createObjectNode();
                    resp.set("result", result);
                    break;
                }
                default:
                    return ResponseEntity.ok(error(resp, -32601, "Method not found: " + method));
            }
        } catch (Exception ex) {
            return ResponseEntity.ok(error(resp, -32000, ex.getMessage() == null ? ex.toString() : ex.getMessage()));
        }
        return ResponseEntity.ok(resp);
    }

    private ObjectNode error(ObjectNode base, int code, String message) {
        ObjectNode err = mapper.createObjectNode();
        err.put("code", code);
        err.put("message", message);
        base.set("error", err);
        return base;
    }

    private ObjectNode tool(String name, String desc, ObjectNode inputSchema) {
        ObjectNode t = mapper.createObjectNode();
        t.put("name", name);
        t.put("description", desc);
        t.set("inputSchema", inputSchema);
        return t;
    }

    private ObjectNode schema(String[] required) {
        ObjectNode schema = mapper.createObjectNode();
        schema.put("type", "object");
        ObjectNode props = mapper.createObjectNode();
        for (String r : required) {
            ObjectNode p = mapper.createObjectNode();
            p.put("type", "string");
            props.set(r, p);
        }
        schema.set("properties", props);
        ArrayNode req = mapper.createArrayNode();
        for (String r : required) req.add(r);
        schema.set("required", req);
        return schema;
    }

    private ObjectNode emptySchema() {
        ObjectNode schema = mapper.createObjectNode();
        schema.put("type", "object");
        schema.set("properties", mapper.createObjectNode());
        return schema;
    }

    private String nonNullText(JsonNode node, String field) {
        if (node != null && node.has(field) && !node.get(field).isNull()) return node.get(field).asText("");
        return "";
    }
}


