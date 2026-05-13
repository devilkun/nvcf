/*
SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	echo "github.com/labstack/echo/v4"

	openairesponses "github.com/NVIDIA/nvcf/src/invocation-plane-services/llm-gateway/api/adapters/openairesponses"
	"github.com/NVIDIA/nvcf/src/invocation-plane-services/llm-gateway/config"
	"github.com/NVIDIA/nvcf/src/invocation-plane-services/llm-gateway/internal/ptr"
	"github.com/NVIDIA/nvcf/src/invocation-plane-services/llm-gateway/models"
	"github.com/NVIDIA/nvcf/src/invocation-plane-services/llm-gateway/provider"
	"github.com/NVIDIA/nvcf/src/invocation-plane-services/llm-gateway/requestctx"
)

func TestCreateResponseDelegatesUnaryThroughChatHandler(t *testing.T) {
	t.Parallel()

	cfg := config.Default()

	handlers := NewHandlers(
		cfg,
		&stubResponsesProvider{
			completeResponse: &models.ChatCompletionResponse{
				ID:        "chatcmpl-123",
				Object:    models.ObjectChatCompletion,
				CreatedAt: 123,
				Model:     "gateway-model",
				Choices: []models.ChatCompletionChoice{
					{
						Index: 0,
						Message: models.ChatCompletionMessage{
							Role:    models.ChatCompletionRoleAssistant,
							Content: ptr.To("hello from gateway"),
						},
						FinishReason: models.FinishReasonStop,
					},
				},
				Usage: models.ChatCompletionUsage{
					PromptTokens:     5,
					CompletionTokens: 3,
					TotalTokens:      8,
				},
			},
		},
		nil,
		nil,
	)

	e := echo.New()
	e.Use(NewContextMiddleware(cfg))
	handlers.AsResponsesHandlers().RegisterRoutes(e.Group(""))

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/responses",
		strings.NewReader(`{"model":"fn-chat/company-name/model-name","input":"hello"}`),
	)
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()

	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"object":"response"`) {
		t.Fatalf("response body missing responses object: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"model":"fn-chat/company-name/model-name"`) {
		t.Fatalf("response body missing model: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"hello from gateway"`) {
		t.Fatalf("response body missing assistant text: %s", rec.Body.String())
	}
}

func TestCreateResponseReturnsBodySessionIDAndUsesItForAffinity(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	provider := &stubResponsesProvider{
		completeResponse: &models.ChatCompletionResponse{
			ID:        "chatcmpl-session",
			Object:    models.ObjectChatCompletion,
			CreatedAt: 123,
			Model:     "gateway-model",
		},
	}
	handlers := NewHandlers(cfg, provider, nil, nil)

	e := echo.New()
	e.Use(NewContextMiddleware(cfg))
	handlers.AsResponsesHandlers().RegisterRoutes(e.Group(""))

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/responses",
		strings.NewReader(`{"model":"fn-chat/company-name/model-name","input":"hello","prompt_cache_key":"body-session"}`),
	)
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	req.Header.Set(HeaderMultiTurnSessionID, "header-session")
	rec := httptest.NewRecorder()

	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get(HeaderMultiTurnSessionID); got != "body-session" {
		t.Fatalf("%s = %q, want body-session", HeaderMultiTurnSessionID, got)
	}

	reqCtx := provider.lastRequestContext()
	if reqCtx == nil {
		t.Fatal("provider did not receive request context")
	}
	if reqCtx.SessionID != "body-session" {
		t.Fatalf("SessionID = %q, want body-session", reqCtx.SessionID)
	}
	if reqCtx.CacheAffinityKey == "" {
		t.Fatal("CacheAffinityKey is empty")
	}
	if strings.Contains(reqCtx.CacheAffinityKey, "body-session") {
		t.Fatalf("CacheAffinityKey leaks raw session ID: %q", reqCtx.CacheAffinityKey)
	}
}

func TestCreateResponseReusesReturnedSessionIDForAffinity(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	provider := &stubResponsesProvider{
		completeResponse: &models.ChatCompletionResponse{
			ID:        "chatcmpl-session",
			Object:    models.ObjectChatCompletion,
			CreatedAt: 123,
			Model:     "gateway-model",
		},
	}
	handlers := NewHandlers(cfg, provider, nil, nil)

	e := echo.New()
	e.Use(NewContextMiddleware(cfg))
	handlers.AsResponsesHandlers().RegisterRoutes(e.Group(""))

	firstReq := httptest.NewRequest(
		http.MethodPost,
		"/v1/responses",
		strings.NewReader(`{"model":"fn-chat/company-name/model-name","input":"hello","prompt_cache_key":"body-session"}`),
	)
	firstReq.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	firstRec := httptest.NewRecorder()

	e.ServeHTTP(firstRec, firstReq)

	if firstRec.Code != http.StatusOK {
		t.Fatalf("first status = %d, want %d: %s", firstRec.Code, http.StatusOK, firstRec.Body.String())
	}
	returnedSessionID := firstRec.Header().Get(HeaderMultiTurnSessionID)
	if returnedSessionID != "body-session" {
		t.Fatalf("first %s = %q, want body-session", HeaderMultiTurnSessionID, returnedSessionID)
	}

	secondReq := httptest.NewRequest(
		http.MethodPost,
		"/v1/responses",
		strings.NewReader(`{"model":"fn-chat/company-name/model-name","input":"follow-up"}`),
	)
	secondReq.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	secondReq.Header.Set(HeaderMultiTurnSessionID, returnedSessionID)
	secondRec := httptest.NewRecorder()

	e.ServeHTTP(secondRec, secondReq)

	if secondRec.Code != http.StatusOK {
		t.Fatalf("second status = %d, want %d: %s", secondRec.Code, http.StatusOK, secondRec.Body.String())
	}
	if got := secondRec.Header().Get(HeaderMultiTurnSessionID); got != returnedSessionID {
		t.Fatalf("second %s = %q, want %q", HeaderMultiTurnSessionID, got, returnedSessionID)
	}

	reqCtxs := provider.requestContexts()
	if len(reqCtxs) != 2 {
		t.Fatalf("provider recorded %d request contexts, want 2", len(reqCtxs))
	}
	if reqCtxs[0].CacheAffinityKey == "" {
		t.Fatal("first CacheAffinityKey is empty")
	}
	if reqCtxs[1].CacheAffinityKey != reqCtxs[0].CacheAffinityKey {
		t.Fatalf("second CacheAffinityKey = %q, want %q", reqCtxs[1].CacheAffinityKey, reqCtxs[0].CacheAffinityKey)
	}
}

func TestCreateResponseReturnsHeaderSessionIDWhenNoBodyID(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	handlers := NewHandlers(
		cfg,
		&stubResponsesProvider{
			completeResponse: &models.ChatCompletionResponse{
				ID:        "chatcmpl-session",
				Object:    models.ObjectChatCompletion,
				CreatedAt: 123,
				Model:     "gateway-model",
			},
		},
		nil,
		nil,
	)

	e := echo.New()
	e.Use(NewContextMiddleware(cfg))
	handlers.AsResponsesHandlers().RegisterRoutes(e.Group(""))

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/responses",
		strings.NewReader(`{"model":"fn-chat/company-name/model-name","input":"hello"}`),
	)
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	req.Header.Set(HeaderMultiTurnSessionID, "header-session")
	rec := httptest.NewRecorder()

	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get(HeaderMultiTurnSessionID); got != "header-session" {
		t.Fatalf("%s = %q, want header-session", HeaderMultiTurnSessionID, got)
	}
}

func TestCreateResponseReturnsGeneratedSessionIDForPayloadFallback(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	handlers := NewHandlers(
		cfg,
		&stubResponsesProvider{
			completeResponse: &models.ChatCompletionResponse{
				ID:        "chatcmpl-session",
				Object:    models.ObjectChatCompletion,
				CreatedAt: 123,
				Model:     "gateway-model",
			},
		},
		nil,
		nil,
	)

	e := echo.New()
	e.Use(NewContextMiddleware(cfg))
	handlers.AsResponsesHandlers().RegisterRoutes(e.Group(""))

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/responses",
		strings.NewReader(`{"model":"fn-chat/company-name/model-name","input":"hello"}`),
	)
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()

	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	got := rec.Header().Get(HeaderMultiTurnSessionID)
	if !strings.HasPrefix(got, "mt:v1:payload:") {
		t.Fatalf("%s = %q, want generated payload session ID", HeaderMultiTurnSessionID, got)
	}
}

func TestCreateResponseRejectsInvalidSessionHeader(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	handlers := NewHandlers(
		cfg,
		&stubResponsesProvider{
			completeResponse: &models.ChatCompletionResponse{
				ID:        "chatcmpl-session",
				Object:    models.ObjectChatCompletion,
				CreatedAt: 123,
				Model:     "gateway-model",
			},
		},
		nil,
		nil,
	)

	e := echo.New()
	e.Use(NewContextMiddleware(cfg))
	handlers.AsResponsesHandlers().RegisterRoutes(e.Group(""))

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/responses",
		strings.NewReader(`{"model":"fn-chat/company-name/model-name","input":"hello"}`),
	)
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	req.Header.Set(HeaderMultiTurnSessionID, "bad\nsession")
	rec := httptest.NewRecorder()

	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestCreateResponseDelegatesStreamThroughChatHandler(t *testing.T) {
	t.Parallel()

	cfg := config.Default()

	handlers := NewHandlers(
		cfg,
		&stubResponsesProvider{
			streamEvents: []provider.StreamEvent{
				{
					Chunk: &models.ChatCompletionChunk{
						Choices: []models.ChatCompletionChunkChoice{
							{
								Delta: models.ChatCompletionChunkDelta{
									Content: ptr.To("hello "),
								},
							},
						},
					},
				},
				{
					Chunk: &models.ChatCompletionChunk{
						Choices: []models.ChatCompletionChunkChoice{
							{
								Delta: models.ChatCompletionChunkDelta{
									Content: ptr.To("world"),
								},
							},
						},
						Usage: &models.ChatCompletionUsage{
							PromptTokens:     5,
							CompletionTokens: 2,
							TotalTokens:      7,
						},
					},
				},
			},
		},
		nil,
		nil,
	)

	e := echo.New()
	e.Use(NewContextMiddleware(cfg))
	handlers.AsResponsesHandlers().RegisterRoutes(e.Group(""))

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/responses",
		strings.NewReader(`{"model":"fn-chat/company-name/model-name","input":"hello","stream":true}`),
	)
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()

	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get(HeaderMultiTurnSessionID); got == "" {
		t.Fatalf("%s response header is empty", HeaderMultiTurnSessionID)
	}
	if !strings.Contains(rec.Body.String(), "event: "+openairesponses.EventTypeResponseCreated) {
		t.Fatalf("stream body missing response.created: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "event: "+openairesponses.EventTypeResponseCompleted) {
		t.Fatalf("stream body missing response.completed: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"model":"fn-chat/company-name/model-name"`) {
		t.Fatalf("stream body missing model: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"text":"hello world"`) {
		t.Fatalf("stream body missing completed assistant text: %s", rec.Body.String())
	}
}

type stubResponsesProvider struct {
	completeResponse *models.ChatCompletionResponse
	streamEvents     []provider.StreamEvent
	mu               sync.RWMutex
	reqCtx           *requestctx.RequestContext
	reqCtxs          []*requestctx.RequestContext
}

func (s *stubResponsesProvider) Complete(
	_ context.Context,
	reqCtx *requestctx.RequestContext,
	_ *provider.NormalizedRequest,
) (*models.ChatCompletionResponse, error) {
	s.recordRequestContext(reqCtx)
	return s.completeResponse, nil
}

func (s *stubResponsesProvider) Stream(
	_ context.Context,
	reqCtx *requestctx.RequestContext,
	_ *provider.NormalizedRequest,
) (<-chan provider.StreamEvent, error) {
	s.recordRequestContext(reqCtx)
	ch := make(chan provider.StreamEvent, len(s.streamEvents))
	go func() {
		defer close(ch)
		for _, event := range s.streamEvents {
			ch <- event
		}
	}()
	return ch, nil
}

func (s *stubResponsesProvider) lastRequestContext() *requestctx.RequestContext {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reqCtx
}

func (s *stubResponsesProvider) requestContexts() []*requestctx.RequestContext {
	s.mu.RLock()
	defer s.mu.RUnlock()
	reqCtxs := make([]*requestctx.RequestContext, len(s.reqCtxs))
	copy(reqCtxs, s.reqCtxs)
	return reqCtxs
}

func (s *stubResponsesProvider) recordRequestContext(reqCtx *requestctx.RequestContext) {
	reqCtx = cloneRequestContext(reqCtx)
	s.mu.Lock()
	s.reqCtx = reqCtx
	s.reqCtxs = append(s.reqCtxs, reqCtx)
	s.mu.Unlock()
}

func cloneRequestContext(reqCtx *requestctx.RequestContext) *requestctx.RequestContext {
	if reqCtx == nil {
		return nil
	}
	clone := *reqCtx
	return &clone
}
