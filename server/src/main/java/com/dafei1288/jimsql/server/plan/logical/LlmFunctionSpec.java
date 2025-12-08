package com.dafei1288.jimsql.server.plan.logical;

import java.util.LinkedHashMap;
import java.util.Map;

// Spec for SELECT ask_llm(...) built-in function
public class LlmFunctionSpec {
  private String prompt;
  private Map<String,String> overrides = new LinkedHashMap<>();
  private String label = "ask_llm"; // output column label

  public String getPrompt() { return prompt; }
  public void setPrompt(String prompt) { this.prompt = prompt; }

  public Map<String,String> getOverrides() { return overrides; }
  public void setOverrides(Map<String,String> overrides) { this.overrides = overrides; }

  public String getLabel() { return label; }
  public void setLabel(String label) { this.label = label; }
}