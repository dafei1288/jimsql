package com.dafei1288.jimsql.server.plan.logical;

public class LlmFunctionSpec {
  private final String prompt;
  private String label = "ask_llm";
  private String model;
  private String baseUrl;
  private String apiKey;
  private String apiType;
  private String temperature;
  private String stream;
  private String thinking;

  public LlmFunctionSpec(String prompt) { this.prompt = prompt; }

  public String getPrompt() { return prompt; }

  public String getLabel() { return label; }
  public void setLabel(String label) { if (label != null && !label.isEmpty()) this.label = label; }

  public String getModel() { return model; }
  public void setModel(String model) { this.model = model; }

  public String getBaseUrl() { return baseUrl; }
  public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }

  public String getApiKey() { return apiKey; }
  public void setApiKey(String apiKey) { this.apiKey = apiKey; }

  public String getApiType() { return apiType; }
  public void setApiType(String apiType) { this.apiType = apiType; }

  public String getTemperature() { return temperature; }
  public void setTemperature(String temperature) { this.temperature = temperature; }

  public String getStream() { return stream; }
  public void setStream(String stream) { this.stream = stream; }

  public String getThinking() { return thinking; }
  public void setThinking(String thinking) { this.thinking = thinking; }
}