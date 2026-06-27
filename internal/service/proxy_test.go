package service

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/WindyPear-Team/flai/internal/model"
	"github.com/shopspring/decimal"
)

func TestRawProviderRequestKeepsClaudeMessagesEndpoint(t *testing.T) {
	channel := &model.Channel{BaseURL: "https://anyrouter.top", APIKey: "upstream-key"}
	originalHeader := http.Header{
		"Authorization": []string{"Bearer user-key"},
		"x-api-key":     []string{"user-key"},
	}

	request := rawProviderRequest(channel, protocolClaude, http.MethodPost, "/v1/messages", []byte(`{}`), originalHeader)

	if request.URL != "https://anyrouter.top/v1/messages" {
		t.Fatalf("rawProviderRequest URL = %q, want Claude messages endpoint", request.URL)
	}
	if request.Header.Get("x-api-key") != "upstream-key" {
		t.Fatalf("x-api-key was not replaced with upstream key")
	}
	if request.Header.Get("Authorization") != "Bearer upstream-key" {
		t.Fatalf("Authorization was not replaced with upstream key")
	}
}

func TestRawProviderRequestKeepsGeminiEndpointAndChannelKey(t *testing.T) {
	channel := &model.Channel{BaseURL: "https://example.com", APIKey: "upstream-key"}
	originalHeader := http.Header{"x-goog-api-key": []string{"user-key"}}

	request := rawProviderRequest(channel, protocolGemini, http.MethodPost, "/v1beta/models/gemini-pro:generateContent", []byte(`{}`), originalHeader)

	if !strings.HasPrefix(request.URL, "https://example.com/v1beta/models/gemini-pro:generateContent?") {
		t.Fatalf("rawProviderRequest URL = %q, want Gemini generateContent endpoint", request.URL)
	}
	if !strings.Contains(request.URL, "key=upstream-key") {
		t.Fatalf("Gemini request URL did not use upstream key: %q", request.URL)
	}
	if strings.Contains(request.URL, "user-key") || request.Header.Get("x-goog-api-key") == "user-key" {
		t.Fatalf("Gemini request leaked user key")
	}
}

func TestPrepareOpenAIImageGenerationRequestRewritesModel(t *testing.T) {
	channel := &model.Channel{BaseURL: "https://example.com/v1", APIKey: "upstream-key"}
	requestBody := map[string]interface{}{
		"model":  "dall-e-3",
		"prompt": "draw a pear",
		"n":      float64(2),
	}

	request, err := prepareOpenAIImageGenerationRequest(channel, "upstream-image-model", requestBody)
	if err != nil {
		t.Fatalf("prepareOpenAIImageGenerationRequest returned error: %v", err)
	}
	if request.URL != "https://example.com/v1/images/generations" {
		t.Fatalf("image generation URL = %q", request.URL)
	}
	if request.Header.Get("Authorization") != "Bearer upstream-key" {
		t.Fatalf("Authorization was not set from channel key")
	}
	if requestBody["model"] != "dall-e-3" {
		t.Fatalf("original request body was mutated")
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(request.Body, &payload); err != nil {
		t.Fatalf("failed to decode prepared body: %v", err)
	}
	if payload["model"] != "upstream-image-model" {
		t.Fatalf("prepared model = %q, want upstream model", payload["model"])
	}
	if payload["prompt"] != "draw a pear" {
		t.Fatalf("prepared prompt was not preserved")
	}
}

func TestPrepareOpenAIVideoGenerationRequestRewritesModel(t *testing.T) {
	channel := &model.Channel{BaseURL: "https://example.com/v1", APIKey: "upstream-key"}
	requestBody := map[string]interface{}{
		"model":  "doubao-seedance",
		"prompt": "make a pear video",
		"n":      float64(2),
	}

	request, err := prepareOpenAIVideoGenerationRequest(channel, "upstream-video-model", requestBody)
	if err != nil {
		t.Fatalf("prepareOpenAIVideoGenerationRequest returned error: %v", err)
	}
	if request.URL != "https://example.com/v1/videos/generations" {
		t.Fatalf("video generation URL = %q", request.URL)
	}
	if request.Header.Get("Authorization") != "Bearer upstream-key" {
		t.Fatalf("Authorization was not set from channel key")
	}
	if requestBody["model"] != "doubao-seedance" {
		t.Fatalf("original request body was mutated")
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(request.Body, &payload); err != nil {
		t.Fatalf("failed to decode prepared body: %v", err)
	}
	if payload["model"] != "upstream-video-model" {
		t.Fatalf("prepared model = %q, want upstream model", payload["model"])
	}
	if payload["prompt"] != "make a pear video" {
		t.Fatalf("prepared prompt was not preserved")
	}
}

func TestEstimateImageUsageUsesResponseImageCount(t *testing.T) {
	requestBody := map[string]interface{}{
		"model":  "gpt-image-1",
		"prompt": "draw a pear",
		"n":      float64(4),
	}
	responseData := map[string]interface{}{
		"data": []interface{}{
			map[string]interface{}{"url": "https://example.com/1.png"},
			map[string]interface{}{"url": "https://example.com/2.png"},
		},
	}

	usage := estimateImageUsageTokens("gpt-image-1", requestBody, responseData)
	if usage.InputTokens <= 0 {
		t.Fatalf("expected prompt input tokens, got %d", usage.InputTokens)
	}
	if usage.OutputTokens != 2000000 {
		t.Fatalf("expected one million output units per returned image, got %d", usage.OutputTokens)
	}
}

func TestEstimateVideoUsageUsesResponseVideoCount(t *testing.T) {
	requestBody := map[string]interface{}{
		"model":  "doubao-seedance",
		"prompt": "make a pear video",
		"n":      float64(4),
	}
	responseData := map[string]interface{}{
		"data": []interface{}{
			map[string]interface{}{"url": "https://example.com/1.mp4"},
			map[string]interface{}{"url": "https://example.com/2.mp4"},
		},
	}

	usage := estimateVideoUsageTokens("doubao-seedance", requestBody, responseData)
	if usage.InputTokens <= 0 {
		t.Fatalf("expected prompt input tokens, got %d", usage.InputTokens)
	}
	if usage.OutputTokens != 2000000 {
		t.Fatalf("expected one million output units per returned video, got %d", usage.OutputTokens)
	}
}

func TestCalculatePerCallUsageCost(t *testing.T) {
	got := calculateModelUsageCost(usageTokenCounts{
		OutputTokens: 2000000,
	}, model.Model{
		QuotaType:   1,
		OutputPrice: decimal.RequireFromString("0.12"),
	})
	want := decimal.RequireFromString("0.24")
	if !got.Equal(want) {
		t.Fatalf("calculateModelUsageCost() = %s, want %s", got.String(), want.String())
	}
}

func TestParseImageTotalUsageTokens(t *testing.T) {
	usage, ok := parseImageTotalUsageTokens(map[string]interface{}{
		"usage": map[string]interface{}{
			"input_tokens": float64(23),
			"total_tokens": float64(123),
		},
	})
	if !ok {
		t.Fatal("expected image usage from total tokens")
	}
	if usage.InputTokens != 23 || usage.OutputTokens != 100 {
		t.Fatalf("unexpected usage: input=%d output=%d", usage.InputTokens, usage.OutputTokens)
	}
}

func TestSelectModelConfigRoundRobin(t *testing.T) {
	service := NewProxyService()
	userChannelID := uint(7)
	candidates := []model.ModelConfig{
		{Channel: model.Channel{ID: 1, UserChannelID: &userChannelID, UserChannel: model.UserChannel{RoutingAlgorithm: RoutingRoundRobin}}},
		{Channel: model.Channel{ID: 2, UserChannelID: &userChannelID, UserChannel: model.UserChannel{RoutingAlgorithm: RoutingRoundRobin}}},
	}

	first := service.selectModelConfig(candidates, "gpt-test")
	second := service.selectModelConfig(candidates, "gpt-test")
	third := service.selectModelConfig(candidates, "gpt-test")

	if first.Channel.ID != 1 || second.Channel.ID != 2 || third.Channel.ID != 1 {
		t.Fatalf("round robin selected channel ids %d, %d, %d", first.Channel.ID, second.Channel.ID, third.Channel.ID)
	}
}

func TestSelectModelConfigWeightedRoundRobin(t *testing.T) {
	service := NewProxyService()
	userChannelID := uint(9)
	candidates := []model.ModelConfig{
		{Channel: model.Channel{ID: 1, UserChannelID: &userChannelID, Weight: 1, UserChannel: model.UserChannel{RoutingAlgorithm: RoutingWeightedRoundRobin}}},
		{Channel: model.Channel{ID: 2, UserChannelID: &userChannelID, Weight: 2, UserChannel: model.UserChannel{RoutingAlgorithm: RoutingWeightedRoundRobin}}},
	}

	ids := []uint{
		service.selectModelConfig(candidates, "gpt-test").Channel.ID,
		service.selectModelConfig(candidates, "gpt-test").Channel.ID,
		service.selectModelConfig(candidates, "gpt-test").Channel.ID,
		service.selectModelConfig(candidates, "gpt-test").Channel.ID,
	}
	want := []uint{1, 2, 2, 1}
	for index, id := range ids {
		if id != want[index] {
			t.Fatalf("weighted round robin selected %v, want %v", ids, want)
		}
	}
}
