package service

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/WindyPear-Team/flai/internal/model"
	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

const (
	advancedChatRunStatusQueued    = "queued"
	advancedChatRunStatusRunning   = "running"
	advancedChatRunStatusCompleted = "completed"
	advancedChatRunStatusFailed    = "failed"
	advancedChatRunStatusCancelled = "cancelled"
)

type AdvancedChatSession struct {
	ID                       string     `gorm:"primaryKey;size:80" json:"id"`
	UserID                   uint       `gorm:"index;not null" json:"user_id"`
	User                     model.User `gorm:"foreignKey:UserID" json:"-"`
	Title                    string     `gorm:"size:200;not null" json:"title"`
	RunMode                  string     `gorm:"size:20;not null" json:"run_mode"`
	AgentID                  string     `gorm:"size:80" json:"agent_id"`
	SkillIDs                 string     `gorm:"type:text;not null" json:"-"`
	MCPServerIDs             string     `gorm:"type:text;not null" json:"-"`
	ConnectorDeviceID        string     `gorm:"size:80" json:"connector_device_id"`
	ConnectorWorkspacePath   string     `gorm:"type:text" json:"connector_workspace_path"`
	ConnectorAutoApprove     bool       `gorm:"default:false" json:"connector_auto_approve"`
	ConnectorCommandPrefixes string     `gorm:"type:text;not null;default:'[]'" json:"-"`
	ModelName                string     `gorm:"size:100" json:"model_name"`
	UserChannelID            uint       `gorm:"index" json:"user_channel_id"`
	MaxTokens                int        `gorm:"default:0" json:"max_tokens"`
	Temperature              *float64   `json:"temperature"`
	ReasoningEffort          string     `gorm:"size:20" json:"reasoning_effort"`
	CreatedAt                time.Time  `json:"created_at"`
	UpdatedAt                time.Time  `json:"updated_at"`
}

type AdvancedChatMessage struct {
	ID           string     `gorm:"primaryKey;size:80" json:"id"`
	SessionID    string     `gorm:"index;not null" json:"session_id"`
	UserID       uint       `gorm:"index;not null" json:"user_id"`
	User         model.User `gorm:"foreignKey:UserID" json:"-"`
	Role         string     `gorm:"size:20;not null" json:"role"`
	Content      string     `gorm:"type:text;not null" json:"content"`
	ContentParts string     `gorm:"type:text;not null;default:'[]'" json:"-"`
	ToolCalls    string     `gorm:"type:text;not null" json:"-"`
	SortOrder    int        `gorm:"index;not null" json:"sort_order"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

type AdvancedChatRun struct {
	ID                 string          `gorm:"primaryKey;size:80" json:"id"`
	SessionID          string          `gorm:"index;not null" json:"session_id"`
	UserID             uint            `gorm:"index;not null" json:"user_id"`
	User               model.User      `gorm:"foreignKey:UserID" json:"-"`
	AssistantMessageID string          `gorm:"size:80;not null" json:"assistant_message_id"`
	Mode               string          `gorm:"size:20;not null" json:"mode"`
	Status             string          `gorm:"size:20;index;not null" json:"status"`
	StatusMessage      string          `gorm:"size:80" json:"status_message"`
	CurrentRound       int             `gorm:"default:0" json:"current_round"`
	ErrorMessage       string          `gorm:"type:text;not null" json:"error_message"`
	Cost               decimal.Decimal `gorm:"type:decimal(20,10);not null" json:"cost"`
	ToolCalls          int             `gorm:"default:0" json:"tool_calls"`
	ToolCallDetails    string          `gorm:"type:text;not null" json:"-"`
	StartedAt          *time.Time      `json:"started_at"`
	FinishedAt         *time.Time      `json:"finished_at"`
	CreatedAt          time.Time       `json:"created_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
}

type AdvancedChatRunEvent struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	RunID     string    `gorm:"uniqueIndex:idx_advanced_chat_run_event_seq;size:80;not null" json:"run_id"`
	SessionID string    `gorm:"index;size:80;not null" json:"session_id"`
	UserID    uint      `gorm:"index;not null" json:"user_id"`
	Seq       int       `gorm:"uniqueIndex:idx_advanced_chat_run_event_seq;not null" json:"seq"`
	Event     string    `gorm:"size:40;not null" json:"event"`
	Payload   string    `gorm:"type:text;not null" json:"-"`
	CreatedAt time.Time `json:"created_at"`
}

type advancedChatMessageResponse struct {
	ID        string                           `json:"id"`
	Role      string                           `json:"role"`
	Content   string                           `json:"content"`
	Parts     []advancedChatContentPart        `json:"content_parts,omitempty"`
	ToolCalls []advancedChatCompletionToolCall `json:"tool_calls,omitempty"`
	CreatedAt time.Time                        `json:"created_at"`
	UpdatedAt time.Time                        `json:"updated_at"`
}

type advancedChatRunResponse struct {
	ID                 string                           `json:"id"`
	SessionID          string                           `json:"session_id"`
	AssistantMessageID string                           `json:"assistant_message_id"`
	Mode               string                           `json:"mode"`
	Status             string                           `json:"status"`
	StatusMessage      string                           `json:"status_message"`
	CurrentRound       int                              `json:"current_round"`
	ErrorMessage       string                           `json:"error_message,omitempty"`
	Cost               decimal.Decimal                  `json:"cost"`
	ToolCalls          int                              `json:"tool_calls"`
	ToolCallDetails    []advancedChatCompletionToolCall `json:"tool_call_details,omitempty"`
	StartedAt          *time.Time                       `json:"started_at,omitempty"`
	FinishedAt         *time.Time                       `json:"finished_at,omitempty"`
	CreatedAt          time.Time                        `json:"created_at"`
	UpdatedAt          time.Time                        `json:"updated_at"`
}

type advancedChatSessionResponse struct {
	ID                       string                        `json:"id"`
	Title                    string                        `json:"title"`
	Messages                 []advancedChatMessageResponse `json:"messages"`
	RunMode                  string                        `json:"run_mode"`
	AgentID                  string                        `json:"agent_id,omitempty"`
	SkillIDs                 []string                      `json:"skill_ids"`
	MCPServerIDs             []string                      `json:"mcp_server_ids"`
	ConnectorDeviceID        string                        `json:"connector_device_id,omitempty"`
	ConnectorWorkspacePath   string                        `json:"connector_workspace_path,omitempty"`
	ConnectorAutoApprove     bool                          `json:"connector_auto_approve"`
	ConnectorCommandPrefixes []string                      `json:"connector_command_prefixes"`
	ModelName                string                        `json:"model_name,omitempty"`
	UserChannelID            uint                          `json:"user_channel_id,omitempty"`
	MaxTokens                int                           `json:"max_tokens,omitempty"`
	Temperature              *float64                      `json:"temperature,omitempty"`
	ReasoningEffort          string                        `json:"reasoning_effort,omitempty"`
	LatestRun                *advancedChatRunResponse      `json:"latest_run,omitempty"`
	CreatedAt                time.Time                     `json:"created_at"`
	UpdatedAt                time.Time                     `json:"updated_at"`
}

type advancedChatRunEventResponse struct {
	ID        uint                   `json:"id"`
	RunID     string                 `json:"run_id"`
	SessionID string                 `json:"session_id"`
	Seq       int                    `json:"seq"`
	Event     string                 `json:"event"`
	Payload   map[string]interface{} `json:"payload"`
	CreatedAt time.Time              `json:"created_at"`
}

type advancedChatSessionInput struct {
	ID                       string                            `json:"id"`
	Title                    string                            `json:"title"`
	RunMode                  string                            `json:"run_mode"`
	AgentID                  string                            `json:"agent_id"`
	SkillIDs                 []string                          `json:"skill_ids"`
	MCPServerIDs             []string                          `json:"mcp_server_ids"`
	ConnectorDeviceID        string                            `json:"connector_device_id"`
	ConnectorWorkspacePath   string                            `json:"connector_workspace_path"`
	ConnectorAutoApprove     bool                              `json:"connector_auto_approve"`
	ConnectorCommandPrefixes []string                          `json:"connector_command_prefixes"`
	ModelName                string                            `json:"model_name"`
	UserChannelID            uint                              `json:"user_channel_id"`
	MaxTokens                int                               `json:"max_tokens"`
	Temperature              *float64                          `json:"temperature"`
	ReasoningEffort          string                            `json:"reasoning_effort"`
	Messages                 []advancedChatSessionMessageInput `json:"messages"`
}

type advancedChatSessionMessageInput struct {
	ID        string                           `json:"id"`
	Role      string                           `json:"role"`
	Content   string                           `json:"content"`
	Parts     []advancedChatContentPart        `json:"content_parts"`
	ToolCalls []advancedChatCompletionToolCall `json:"tool_calls"`
}

type preparedAdvancedChatAssistantRun struct {
	input                    advancedChatCompletionInput
	messages                 []advancedChatCompletionMessage
	modelName                string
	mode                     string
	runID                    string
	maxToolRounds            int
	agent                    *AdvancedChatAgent
	skills                   []AdvancedChatSkill
	workspaceSkills          []advancedChatWorkspaceSkill
	agentGroups              []advancedChatAgentGroup
	servers                  []AdvancedChatMCPServer
	connectorDevice          *AdvancedChatConnectorDevice
	connectorWorkspace       string
	connectorAutoApprove     bool
	connectorCommandPrefixes []string
	delivery                 *AdvancedChatDelivery
	timeout                  time.Duration
}

type advancedChatCompletionObserver struct {
	OnStatus   func(payload gin.H) error
	OnText     func(delta string, round int) error
	OnToolCall func(detail advancedChatCompletionToolCall) error
}

type advancedChatContentPart struct {
	Round   int    `json:"round,omitempty"`
	Content string `json:"content"`
}

func (api *advancedChatAPI) listSessions(c *gin.Context) {
	user, ok := currentAdvancedChatUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}
	sessions, err := listAdvancedChatSessionResponses(user.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list sessions"})
		return
	}
	c.JSON(http.StatusOK, sessions)
}

func (api *advancedChatAPI) getSession(c *gin.Context) {
	user, ok := currentAdvancedChatUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}
	session, err := advancedChatSessionResponseFor(user.ID, c.Param("id"))
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Session not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load session"})
		return
	}
	c.JSON(http.StatusOK, session)
}

func (api *advancedChatAPI) saveSession(c *gin.Context) {
	user, ok := currentAdvancedChatUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}
	var input advancedChatSessionInput
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	sessionID := normalizeAdvancedChatSessionID(c.Param("id"))
	if sessionID == "" {
		sessionID = normalizeAdvancedChatSessionID(input.ID)
	}
	if sessionID == "" {
		sessionID = newAdvancedChatID("acs")
	}
	session, status, message, err := saveAdvancedChatSessionSnapshot(user.ID, sessionID, input, true)
	if err != nil {
		c.JSON(status, gin.H{"error": message})
		return
	}
	c.JSON(http.StatusOK, session)
}

func (api *advancedChatAPI) deleteSession(c *gin.Context) {
	user, ok := currentAdvancedChatUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}
	sessionID := strings.TrimSpace(c.Param("id"))
	err := model.DB.Transaction(func(tx *gorm.DB) error {
		var runs []AdvancedChatRun
		if err := tx.Where("session_id = ? AND user_id = ?", sessionID, user.ID).Find(&runs).Error; err != nil {
			return err
		}
		for _, run := range runs {
			if err := tx.Where("run_id = ? AND user_id = ?", run.ID, user.ID).Delete(&AdvancedChatRunEvent{}).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("session_id = ? AND user_id = ?", sessionID, user.ID).Delete(&AdvancedChatRun{}).Error; err != nil {
			return err
		}
		if err := tx.Where("session_id = ? AND user_id = ?", sessionID, user.ID).Delete(&AdvancedChatMessage{}).Error; err != nil {
			return err
		}
		return tx.Where("id = ? AND user_id = ?", sessionID, user.ID).Delete(&AdvancedChatSession{}).Error
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete session"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Session deleted"})
}

func (api *advancedChatAPI) getRun(c *gin.Context) {
	user, ok := currentAdvancedChatUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}
	var run AdvancedChatRun
	if err := model.DB.Where("id = ? AND user_id = ?", c.Param("id"), user.ID).First(&run).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Run not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load run"})
		return
	}
	c.JSON(http.StatusOK, advancedChatRunResponseFromModel(run))
}

func (api *advancedChatAPI) stopRun(c *gin.Context) {
	user, ok := currentAdvancedChatUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}
	run, status, message, err := stopAdvancedChatRun(c.Param("id"), user.ID)
	if err != nil {
		c.JSON(status, gin.H{"error": message})
		return
	}
	c.JSON(http.StatusOK, advancedChatRunResponseFromModel(run))
}

func (api *advancedChatAPI) listRunEvents(c *gin.Context) {
	user, ok := currentAdvancedChatUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}
	after, _ := strconv.Atoi(c.Query("after"))
	var events []AdvancedChatRunEvent
	if err := model.DB.
		Where("run_id = ? AND user_id = ? AND seq > ?", c.Param("id"), user.ID, after).
		Order("seq ASC").
		Limit(200).
		Find(&events).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load run events"})
		return
	}
	result := make([]advancedChatRunEventResponse, 0, len(events))
	for _, event := range events {
		result = append(result, advancedChatRunEventResponseFromModel(event))
	}
	c.JSON(http.StatusOK, result)
}

func stopAdvancedChatRun(rawRunID string, userID uint) (AdvancedChatRun, int, string, error) {
	runID := strings.TrimSpace(rawRunID)
	var run AdvancedChatRun
	if err := model.DB.Where("id = ? AND user_id = ?", runID, userID).First(&run).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return run, http.StatusNotFound, "Run not found", err
		}
		return run, http.StatusInternalServerError, "Failed to load run", err
	}
	if !advancedChatRunIsActive(run.Status) {
		return run, http.StatusOK, "", nil
	}

	now := time.Now()
	stopped := false
	err := model.DB.Transaction(func(tx *gorm.DB) error {
		update := tx.Model(&AdvancedChatRun{}).
			Where("id = ? AND user_id = ? AND status IN ?", run.ID, userID, []string{advancedChatRunStatusQueued, advancedChatRunStatusRunning}).
			Updates(map[string]interface{}{
				"status":         advancedChatRunStatusCancelled,
				"status_message": "cancelled",
				"error_message":  "",
				"finished_at":    &now,
				"updated_at":     now,
			})
		if update.Error != nil {
			return update.Error
		}
		if update.RowsAffected == 0 {
			return nil
		}
		if err := tx.Model(&AdvancedChatMessage{}).
			Where("id = ? AND user_id = ? AND content = ?", run.AssistantMessageID, userID, "").
			Update("content", "Stopped").Error; err != nil {
			return err
		}
		taskUpdate := tx.Model(&AdvancedChatConnectorTask{}).
			Where("run_id = ? AND user_id = ? AND status IN ?", run.ID, userID, []string{
				advancedChatConnectorTaskStatusPendingApproval,
				advancedChatConnectorTaskStatusQueued,
				advancedChatConnectorTaskStatusRunning,
			}).
			Updates(map[string]interface{}{
				"status":        advancedChatConnectorTaskStatusFailed,
				"error_message": "cancelled by user",
				"finished_at":   &now,
				"updated_at":    now,
			})
		if taskUpdate.Error != nil {
			return taskUpdate.Error
		}
		stopped = true
		return nil
	})
	if err != nil {
		return run, http.StatusInternalServerError, "Failed to stop run", err
	}
	if stopped {
		if cancel, ok := advancedChatRunCancels.Load(run.ID); ok {
			if fn, ok := cancel.(context.CancelFunc); ok {
				fn()
			}
		}
		_ = appendAdvancedChatRunEvent(run.ID, run.SessionID, userID, "status", gin.H{"message": "cancelled"})
	}
	if err := model.DB.Where("id = ? AND user_id = ?", run.ID, userID).First(&run).Error; err != nil {
		return run, http.StatusInternalServerError, "Failed to load stopped run", err
	}
	return run, http.StatusOK, "", nil
}

func advancedChatRunIsActive(status string) bool {
	return status == advancedChatRunStatusQueued || status == advancedChatRunStatusRunning
}

func ensureAdvancedChatRunNotCancelled(runID string, userID uint) error {
	var run AdvancedChatRun
	if err := model.DB.Select("status").Where("id = ? AND user_id = ?", runID, userID).First(&run).Error; err != nil {
		return err
	}
	if run.Status == advancedChatRunStatusCancelled {
		return errAdvancedChatRunCancelled
	}
	return nil
}

func (api *advancedChatAPI) startAssistantCompletionRun(c *gin.Context, user *model.User, input advancedChatCompletionInput, messages []advancedChatCompletionMessage, modelName string) {
	prepared, status, message, err := prepareAdvancedChatAssistantRun(c.Request.Context(), user.ID, input, messages, modelName)
	if err != nil {
		c.JSON(status, gin.H{"error": message})
		return
	}
	session, run, status, message, err := createAdvancedChatAssistantRun(user.ID, prepared)
	if err != nil {
		c.JSON(status, gin.H{"error": message})
		return
	}
	go runAdvancedChatAssistantCompletion(run.ID, user.ID, prepared)
	c.JSON(http.StatusAccepted, gin.H{"session": session, "run": run})
}

func prepareAdvancedChatAssistantRun(ctx context.Context, userID uint, input advancedChatCompletionInput, messages []advancedChatCompletionMessage, modelName string) (preparedAdvancedChatAssistantRun, int, string, error) {
	if !advancedChatAssistantModeEnabled() {
		return preparedAdvancedChatAssistantRun{}, http.StatusForbidden, "Assistant mode is disabled", errors.New("assistant mode disabled")
	}
	agent, err := loadAdvancedChatAgent(userID, input.AgentID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return preparedAdvancedChatAssistantRun{}, http.StatusBadRequest, "Agent not found", err
		}
		return preparedAdvancedChatAssistantRun{}, http.StatusInternalServerError, "Failed to load agent", err
	}
	skills, err := loadAdvancedChatSkills(userID, input.SkillIDs)
	if err != nil {
		return preparedAdvancedChatAssistantRun{}, http.StatusInternalServerError, "Failed to load skills", err
	}
	if len(skills) != len(uniqueStringsLocal(input.SkillIDs)) {
		return preparedAdvancedChatAssistantRun{}, http.StatusBadRequest, "Unknown skill", errors.New("unknown skill")
	}
	serverIDs := uniqueStringsLocal(append(input.MCPServerIDs, skillMCPIDs(skills)...))
	servers, err := loadAdvancedChatMCPServersForCall(userID, serverIDs)
	if len(serverIDs) > 0 && !advancedChatAssistantMCPToolsEnabled() {
		return preparedAdvancedChatAssistantRun{}, http.StatusBadRequest, "MCP tools are disabled", errors.New("mcp tools disabled")
	}
	if err != nil {
		return preparedAdvancedChatAssistantRun{}, http.StatusBadRequest, err.Error(), err
	}
	if !advancedChatAssistantMCPToolsEnabled() {
		servers = []AdvancedChatMCPServer{}
	}
	if (strings.TrimSpace(input.ConnectorDeviceID) != "" || strings.TrimSpace(input.ConnectorWorkspacePath) != "") && !advancedChatAssistantConnectorToolsEnabled() {
		return preparedAdvancedChatAssistantRun{}, http.StatusBadRequest, "Workspace tools are disabled", errors.New("workspace tools disabled")
	}
	connectorDevice, connectorWorkspace, err := loadAdvancedChatConnectorForRun(userID, input.ConnectorDeviceID, input.ConnectorWorkspacePath)
	if err != nil {
		return preparedAdvancedChatAssistantRun{}, http.StatusBadRequest, err.Error(), err
	}
	workspaceSkills := []advancedChatWorkspaceSkill{}
	if connectorDevice != nil {
		workspaceSkills, err = loadAdvancedChatWorkspaceSkillsForRun(ctx, userID, connectorDevice, connectorWorkspace)
		if err != nil {
			return preparedAdvancedChatAssistantRun{}, http.StatusBadRequest, err.Error(), err
		}
	}
	agentGroups := []advancedChatAgentGroup{}
	if connectorDevice != nil {
		if loaded, loadErr := loadAdvancedChatAgentGroupsForRun(ctx, userID, connectorDevice); loadErr == nil {
			agentGroups = loaded
		}
	}
	input.ConnectorDeviceID = strings.TrimSpace(input.ConnectorDeviceID)
	input.ConnectorWorkspacePath = connectorWorkspace
	input.ConnectorCommandPrefixes = normalizeConnectorCommandPrefixes(input.ConnectorCommandPrefixes)
	mode := advancedChatModeAssistant
	input.Mode = mode
	return preparedAdvancedChatAssistantRun{
		input:                    input,
		messages:                 messages,
		modelName:                modelName,
		mode:                     mode,
		maxToolRounds:            advancedChatCompletionMaxToolRounds(mode),
		agent:                    agent,
		skills:                   skills,
		workspaceSkills:          workspaceSkills,
		agentGroups:              agentGroups,
		servers:                  servers,
		connectorDevice:          connectorDevice,
		connectorWorkspace:       connectorWorkspace,
		connectorAutoApprove:     input.ConnectorAutoApprove,
		connectorCommandPrefixes: input.ConnectorCommandPrefixes,
	}, http.StatusOK, "", nil
}

func saveAdvancedChatSessionSnapshot(userID uint, sessionID string, input advancedChatSessionInput, replaceMessages bool) (advancedChatSessionResponse, int, string, error) {
	sessionID = normalizeAdvancedChatSessionID(sessionID)
	if sessionID == "" {
		return advancedChatSessionResponse{}, http.StatusBadRequest, "Invalid session id", errors.New("invalid session id")
	}
	runMode := normalizeAdvancedChatCompletionMode(input.RunMode)
	if runMode == advancedChatModeAssistant && !advancedChatAssistantModeEnabled() {
		return advancedChatSessionResponse{}, http.StatusForbidden, "Assistant mode is disabled", errors.New("assistant mode disabled")
	}
	modelName := strings.TrimSpace(input.ModelName)
	if len([]rune(modelName)) > 100 {
		return advancedChatSessionResponse{}, http.StatusBadRequest, "Model name is too long", errors.New("model name too long")
	}
	maxTokens := normalizeAdvancedChatMaxTokens(input.MaxTokens)
	temperature := normalizeAdvancedChatTemperature(input.Temperature)
	reasoningEffort := normalizeAdvancedChatReasoningEffort(input.ReasoningEffort)
	agentID := strings.TrimSpace(input.AgentID)
	if agentID != "" {
		if _, err := loadAdvancedChatAgent(userID, agentID); err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return advancedChatSessionResponse{}, http.StatusBadRequest, "Agent not found", err
			}
			return advancedChatSessionResponse{}, http.StatusInternalServerError, "Failed to load agent", err
		}
	}
	skillIDs := uniqueStringsLocal(input.SkillIDs)
	skills := []AdvancedChatSkill{}
	if len(skillIDs) > 0 {
		var err error
		skills, err = loadAdvancedChatSkills(userID, skillIDs)
		if err != nil {
			return advancedChatSessionResponse{}, http.StatusInternalServerError, "Failed to load skills", err
		}
		if len(skills) != len(skillIDs) {
			return advancedChatSessionResponse{}, http.StatusBadRequest, "Unknown skill", errors.New("unknown skill")
		}
	}
	mcpServerIDs := uniqueStringsLocal(input.MCPServerIDs)
	if runMode == advancedChatModeAssistant && !advancedChatAssistantMCPToolsEnabled() && (len(mcpServerIDs) > 0 || len(skillMCPIDs(skills)) > 0) {
		return advancedChatSessionResponse{}, http.StatusBadRequest, "MCP tools are disabled", errors.New("mcp tools disabled")
	}
	if len(mcpServerIDs) > 0 {
		if _, err := loadAdvancedChatMCPServersForCall(userID, mcpServerIDs); err != nil {
			return advancedChatSessionResponse{}, http.StatusBadRequest, err.Error(), err
		}
	}
	commandPrefixes := normalizeConnectorCommandPrefixes(input.ConnectorCommandPrefixes)
	commandPrefixesJSON, _ := json.Marshal(commandPrefixes)
	connectorDeviceID := strings.TrimSpace(input.ConnectorDeviceID)
	connectorWorkspacePath := strings.TrimSpace(input.ConnectorWorkspacePath)
	if (connectorDeviceID != "" || connectorWorkspacePath != "") && runMode == advancedChatModeAssistant && !advancedChatAssistantConnectorToolsEnabled() {
		return advancedChatSessionResponse{}, http.StatusBadRequest, "Workspace tools are disabled", errors.New("workspace tools disabled")
	}
	if connectorDeviceID != "" || connectorWorkspacePath != "" {
		if _, workspacePath, err := loadAdvancedChatConnectorForSession(userID, connectorDeviceID, connectorWorkspacePath); err != nil {
			return advancedChatSessionResponse{}, http.StatusBadRequest, err.Error(), err
		} else {
			connectorWorkspacePath = workspacePath
		}
	}
	title := strings.TrimSpace(input.Title)
	if title == "" {
		title = advancedChatTitleFromSessionMessages(input.Messages)
	}
	if title == "" {
		title = "New session"
	}
	if len([]rune(title)) > 200 {
		title = string([]rune(title)[:200])
	}
	skillIDsJSON, _ := json.Marshal(skillIDs)
	mcpServerIDsJSON, _ := json.Marshal(mcpServerIDs)

	err := model.DB.Transaction(func(tx *gorm.DB) error {
		var existingByID AdvancedChatSession
		if err := tx.Where("id = ?", sessionID).Limit(1).Find(&existingByID).Error; err != nil {
			return err
		}
		if existingByID.ID != "" && existingByID.UserID != userID {
			return errAdvancedChatSessionConflict
		}
		if replaceMessages {
			var activeRuns int64
			if err := tx.Model(&AdvancedChatRun{}).
				Where("session_id = ? AND user_id = ? AND status IN ?", sessionID, userID, []string{advancedChatRunStatusQueued, advancedChatRunStatusRunning}).
				Count(&activeRuns).Error; err != nil {
				return err
			}
			if activeRuns > 0 {
				return errAdvancedChatRunActive
			}
		}
		session := AdvancedChatSession{
			ID:                       sessionID,
			UserID:                   userID,
			Title:                    title,
			RunMode:                  runMode,
			AgentID:                  agentID,
			SkillIDs:                 string(skillIDsJSON),
			MCPServerIDs:             string(mcpServerIDsJSON),
			ConnectorDeviceID:        connectorDeviceID,
			ConnectorWorkspacePath:   connectorWorkspacePath,
			ConnectorAutoApprove:     input.ConnectorAutoApprove,
			ConnectorCommandPrefixes: string(commandPrefixesJSON),
			ModelName:                modelName,
			UserChannelID:            input.UserChannelID,
			MaxTokens:                maxTokens,
			Temperature:              temperature,
			ReasoningEffort:          reasoningEffort,
		}
		var existing AdvancedChatSession
		if err := tx.Where("id = ? AND user_id = ?", sessionID, userID).Limit(1).Find(&existing).Error; err != nil {
			return err
		}
		if existing.ID != "" {
			if err := tx.Model(&existing).Updates(map[string]interface{}{
				"title":                      session.Title,
				"run_mode":                   session.RunMode,
				"agent_id":                   session.AgentID,
				"skill_ids":                  session.SkillIDs,
				"mcp_server_ids":             session.MCPServerIDs,
				"connector_device_id":        session.ConnectorDeviceID,
				"connector_workspace_path":   session.ConnectorWorkspacePath,
				"connector_auto_approve":     session.ConnectorAutoApprove,
				"connector_command_prefixes": session.ConnectorCommandPrefixes,
				"model_name":                 session.ModelName,
				"user_channel_id":            session.UserChannelID,
				"max_tokens":                 session.MaxTokens,
				"temperature":                session.Temperature,
				"reasoning_effort":           session.ReasoningEffort,
			}).Error; err != nil {
				return err
			}
		} else {
			if err := tx.Create(&session).Error; err != nil {
				return err
			}
		}
		if !replaceMessages {
			return nil
		}
		if err := tx.Where("session_id = ? AND user_id = ?", sessionID, userID).Delete(&AdvancedChatMessage{}).Error; err != nil {
			return err
		}
		now := time.Now()
		for index, message := range input.Messages {
			role := strings.ToLower(strings.TrimSpace(message.Role))
			if role != "assistant" {
				role = "user"
			}
			toolCalls, err := json.Marshal(message.ToolCalls)
			if err != nil {
				return err
			}
			contentParts, err := json.Marshal(normalizeAdvancedChatContentParts(message.Parts, message.Content))
			if err != nil {
				return err
			}
			id := normalizeAdvancedChatSessionID(message.ID)
			if id == "" {
				id = newAdvancedChatID("acm")
			}
			row := AdvancedChatMessage{
				ID:           id,
				SessionID:    sessionID,
				UserID:       userID,
				Role:         role,
				Content:      message.Content,
				ContentParts: string(contentParts),
				ToolCalls:    string(toolCalls),
				SortOrder:    index,
				CreatedAt:    now.Add(time.Duration(index) * time.Millisecond),
				UpdatedAt:    now.Add(time.Duration(index) * time.Millisecond),
			}
			if err := tx.Create(&row).Error; err != nil {
				return err
			}
		}
		return tx.Model(&AdvancedChatSession{}).Where("id = ? AND user_id = ?", sessionID, userID).Update("updated_at", time.Now()).Error
	})
	if err != nil {
		switch {
		case errors.Is(err, errAdvancedChatRunActive):
			return advancedChatSessionResponse{}, http.StatusConflict, "This session already has a running assistant run", err
		case errors.Is(err, errAdvancedChatSessionConflict):
			return advancedChatSessionResponse{}, http.StatusConflict, "Session id is already used", err
		default:
			return advancedChatSessionResponse{}, http.StatusInternalServerError, "Failed to save session", err
		}
	}
	session, err := advancedChatSessionResponseFor(userID, sessionID)
	if err != nil {
		return advancedChatSessionResponse{}, http.StatusInternalServerError, "Failed to load session", err
	}
	return session, http.StatusOK, "", nil
}

func createAdvancedChatAssistantRun(userID uint, prepared preparedAdvancedChatAssistantRun) (advancedChatSessionResponse, advancedChatRunResponse, int, string, error) {
	sessionID := normalizeAdvancedChatSessionID(prepared.input.SessionID)
	if sessionID == "" {
		sessionID = newAdvancedChatID("acs")
	}
	runID := newAdvancedChatID("acr")
	assistantMessageID := newAdvancedChatID("acm")
	title := strings.TrimSpace(prepared.input.Title)
	if title == "" {
		title = advancedChatTitleFromMessages(prepared.messages)
	}
	if title == "" {
		title = "Assistant session"
	}
	if len([]rune(title)) > 200 {
		title = string([]rune(title)[:200])
	}
	skillIDs, _ := json.Marshal(uniqueStringsLocal(prepared.input.SkillIDs))
	mcpServerIDs, _ := json.Marshal(uniqueStringsLocal(prepared.input.MCPServerIDs))
	commandPrefixes, _ := json.Marshal(normalizeConnectorCommandPrefixes(prepared.input.ConnectorCommandPrefixes))
	emptyToolCalls := "[]"
	now := time.Now()

	var sessionResp advancedChatSessionResponse
	var runResp advancedChatRunResponse
	err := model.DB.Transaction(func(tx *gorm.DB) error {
		var existingByID AdvancedChatSession
		if err := tx.Where("id = ?", sessionID).Limit(1).Find(&existingByID).Error; err != nil {
			return err
		}
		if existingByID.ID != "" && existingByID.UserID != userID {
			return errAdvancedChatSessionConflict
		}

		var activeRuns int64
		if err := tx.Model(&AdvancedChatRun{}).
			Where("session_id = ? AND user_id = ? AND status IN ?", sessionID, userID, []string{advancedChatRunStatusQueued, advancedChatRunStatusRunning}).
			Count(&activeRuns).Error; err != nil {
			return err
		}
		if activeRuns > 0 {
			return errAdvancedChatRunActive
		}

		session := AdvancedChatSession{
			ID:                       sessionID,
			UserID:                   userID,
			Title:                    title,
			RunMode:                  advancedChatModeAssistant,
			AgentID:                  strings.TrimSpace(prepared.input.AgentID),
			SkillIDs:                 string(skillIDs),
			MCPServerIDs:             string(mcpServerIDs),
			ConnectorDeviceID:        strings.TrimSpace(prepared.input.ConnectorDeviceID),
			ConnectorWorkspacePath:   prepared.connectorWorkspace,
			ConnectorAutoApprove:     prepared.input.ConnectorAutoApprove,
			ConnectorCommandPrefixes: string(commandPrefixes),
			ModelName:                prepared.modelName,
			UserChannelID:            prepared.input.UserChannelID,
			MaxTokens:                normalizeAdvancedChatMaxTokens(prepared.input.MaxTokens),
			Temperature:              normalizeAdvancedChatTemperature(prepared.input.Temperature),
			ReasoningEffort:          normalizeAdvancedChatReasoningEffort(prepared.input.ReasoningEffort),
		}
		var existing AdvancedChatSession
		if err := tx.Where("id = ? AND user_id = ?", sessionID, userID).Limit(1).Find(&existing).Error; err != nil {
			return err
		}
		if existing.ID != "" {
			if err := tx.Model(&existing).Updates(map[string]interface{}{
				"title":                      session.Title,
				"run_mode":                   session.RunMode,
				"agent_id":                   session.AgentID,
				"skill_ids":                  session.SkillIDs,
				"mcp_server_ids":             session.MCPServerIDs,
				"connector_device_id":        session.ConnectorDeviceID,
				"connector_workspace_path":   session.ConnectorWorkspacePath,
				"connector_auto_approve":     session.ConnectorAutoApprove,
				"connector_command_prefixes": session.ConnectorCommandPrefixes,
				"model_name":                 session.ModelName,
				"user_channel_id":            session.UserChannelID,
				"max_tokens":                 session.MaxTokens,
				"temperature":                session.Temperature,
				"reasoning_effort":           session.ReasoningEffort,
			}).Error; err != nil {
				return err
			}
		} else {
			if err := tx.Create(&session).Error; err != nil {
				return err
			}
		}

		if err := tx.Where("session_id = ? AND user_id = ?", sessionID, userID).Delete(&AdvancedChatMessage{}).Error; err != nil {
			return err
		}
		for index, message := range prepared.messages {
			messageID := normalizeAdvancedChatSessionID(message.ID)
			if messageID == "" {
				messageID = newAdvancedChatID("acm")
			}
			toolCalls, err := json.Marshal(message.ToolCalls)
			if err != nil {
				return err
			}
			contentParts, err := json.Marshal(normalizeAdvancedChatContentParts(message.Parts, message.Content))
			if err != nil {
				return err
			}
			row := AdvancedChatMessage{
				ID:           messageID,
				SessionID:    sessionID,
				UserID:       userID,
				Role:         message.Role,
				Content:      message.Content,
				ContentParts: string(contentParts),
				ToolCalls:    string(toolCalls),
				SortOrder:    index,
				CreatedAt:    now.Add(time.Duration(index) * time.Millisecond),
				UpdatedAt:    now.Add(time.Duration(index) * time.Millisecond),
			}
			if err := tx.Create(&row).Error; err != nil {
				return err
			}
		}
		assistantMessage := AdvancedChatMessage{
			ID:           assistantMessageID,
			SessionID:    sessionID,
			UserID:       userID,
			Role:         "assistant",
			Content:      "",
			ContentParts: "[]",
			ToolCalls:    emptyToolCalls,
			SortOrder:    len(prepared.messages),
			CreatedAt:    now.Add(time.Duration(len(prepared.messages)) * time.Millisecond),
			UpdatedAt:    now.Add(time.Duration(len(prepared.messages)) * time.Millisecond),
		}
		if err := tx.Create(&assistantMessage).Error; err != nil {
			return err
		}
		run := AdvancedChatRun{
			ID:                 runID,
			SessionID:          sessionID,
			UserID:             userID,
			AssistantMessageID: assistantMessageID,
			Mode:               advancedChatModeAssistant,
			Status:             advancedChatRunStatusQueued,
			StatusMessage:      "assistant_started",
			ErrorMessage:       "",
			Cost:               decimal.Zero,
			ToolCallDetails:    emptyToolCalls,
		}
		if err := tx.Create(&run).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		switch {
		case errors.Is(err, errAdvancedChatRunActive):
			return sessionResp, runResp, http.StatusConflict, "This session already has a running assistant run", err
		case errors.Is(err, errAdvancedChatSessionConflict):
			return sessionResp, runResp, http.StatusConflict, "Session id is already used", err
		default:
			return sessionResp, runResp, http.StatusInternalServerError, "Failed to create assistant run", err
		}
	}
	sessionResp, err = advancedChatSessionResponseFor(userID, sessionID)
	if err != nil {
		return sessionResp, runResp, http.StatusInternalServerError, "Failed to load assistant session", err
	}
	var run AdvancedChatRun
	if err := model.DB.Where("id = ? AND user_id = ?", runID, userID).First(&run).Error; err != nil {
		return sessionResp, runResp, http.StatusInternalServerError, "Failed to load assistant run", err
	}
	runResp = advancedChatRunResponseFromModel(run)
	return sessionResp, runResp, http.StatusAccepted, "", nil
}

var (
	errAdvancedChatRunActive       = errors.New("advanced chat run active")
	errAdvancedChatSessionConflict = errors.New("advanced chat session conflict")
	errAdvancedChatRunCancelled    = errors.New("advanced chat run cancelled")
	advancedChatRunCancels         sync.Map
)

func runAdvancedChatAssistantCompletion(runID string, userID uint, prepared preparedAdvancedChatAssistantRun) {
	timeout := advancedChatCompletionTimeout(advancedChatModeAssistant)
	if prepared.timeout > 0 {
		timeout = prepared.timeout
	}
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), timeout)
	defer timeoutCancel()
	ctx, cancel := context.WithCancel(timeoutCtx)
	defer cancel()
	advancedChatRunCancels.Store(runID, cancel)
	defer advancedChatRunCancels.Delete(runID)
	prepared.runID = runID

	var run AdvancedChatRun
	if err := model.DB.Where("id = ? AND user_id = ?", runID, userID).First(&run).Error; err != nil {
		return
	}
	now := time.Now()
	startUpdate := model.DB.Model(&run).
		Where("status = ?", advancedChatRunStatusQueued).
		Updates(map[string]interface{}{
			"status":         advancedChatRunStatusRunning,
			"status_message": "assistant_started",
			"started_at":     &now,
		})
	if startUpdate.Error != nil || startUpdate.RowsAffected == 0 {
		return
	}
	appendAdvancedChatRunEvent(run.ID, run.SessionID, userID, "status", gin.H{"message": "assistant_started"})

	var user model.User
	if err := model.DB.First(&user, userID).Error; err != nil {
		failAdvancedChatRun(run.ID, run.SessionID, userID, run.AssistantMessageID, "Failed to load user: "+err.Error())
		return
	}
	observer := advancedChatCompletionObserver{
		OnStatus: func(payload gin.H) error {
			if err := ensureAdvancedChatRunNotCancelled(run.ID, userID); err != nil {
				return err
			}
			message, _ := payload["message"].(string)
			round := 0
			if value, ok := payload["round"].(int); ok {
				round = value
			}
			statusMessage := message
			if message == "retrying" {
				attempt, _ := payload["attempt"].(int)
				maxAttempts, _ := payload["max"].(int)
				if attempt > 0 && maxAttempts > 0 {
					statusMessage = fmt.Sprintf("retrying:%d/%d", attempt, maxAttempts)
				}
			}
			updates := map[string]interface{}{"status_message": statusMessage}
			if round > 0 {
				updates["current_round"] = round
			}
			if err := model.DB.Model(&AdvancedChatRun{}).Where("id = ? AND user_id = ?", run.ID, userID).Updates(updates).Error; err != nil {
				return err
			}
			return appendAdvancedChatRunEvent(run.ID, run.SessionID, userID, "status", payload)
		},
		OnText: func(delta string, round int) error {
			if err := ensureAdvancedChatRunNotCancelled(run.ID, userID); err != nil {
				return err
			}
			if err := appendAdvancedChatRunEvent(run.ID, run.SessionID, userID, "text", gin.H{"delta": delta, "round": round}); err != nil {
				return err
			}
			_ = appendAdvancedChatAssistantContent(run.AssistantMessageID, userID, delta, round)
			return nil
		},
		OnToolCall: func(detail advancedChatCompletionToolCall) error {
			if err := ensureAdvancedChatRunNotCancelled(run.ID, userID); err != nil {
				return err
			}
			if err := mergeAdvancedChatRunToolCall(run.ID, userID, run.AssistantMessageID, detail); err != nil {
				return err
			}
			return appendAdvancedChatRunEvent(run.ID, run.SessionID, userID, "tool_call", detail)
		},
	}

	response, err := executePreparedAdvancedChatCompletion(ctx, &user, prepared, observer, true)
	if err != nil {
		if errors.Is(err, errAdvancedChatRunCancelled) || errors.Is(err, context.Canceled) {
			return
		}
		failAdvancedChatRun(run.ID, run.SessionID, userID, run.AssistantMessageID, errorMessageFromAdvancedChatCompletion(err))
		return
	}
	finishAdvancedChatRun(run.ID, run.SessionID, userID, run.AssistantMessageID, response)
}

func executePreparedAdvancedChatCompletion(ctx context.Context, user *model.User, prepared preparedAdvancedChatAssistantRun, observer advancedChatCompletionObserver, stream bool) (*advancedChatCompletionResponse, error) {
	if observer.OnStatus != nil {
		if err := observer.OnStatus(gin.H{"message": "loading_tools"}); err != nil {
			return nil, err
		}
	}
	tools := []ChatExecutorTool{}
	bindings := map[string]mcpToolBinding{}
	if advancedChatAssistantMCPToolsEnabled() {
		var err error
		tools, bindings, err = listAdvancedChatMCPTools(ctx, prepared.servers)
		if err != nil {
			return nil, fmt.Errorf("Failed to load MCP tools: %w", err)
		}
	}
	connectorTools, connectorBindings := advancedChatConnectorTools(prepared.connectorDevice, prepared.connectorWorkspace, prepared.connectorAutoApprove, prepared.connectorCommandPrefixes)
	if len(connectorTools) > 0 {
		tools = append(tools, connectorTools...)
	}
	if len(prepared.agentGroups) > 0 {
		tools = append(tools, advancedChatAgentDelegateTool(prepared.agentGroups))
	}
	deliveryToolName := ""
	if prepared.delivery != nil {
		deliveryToolName = "deliver_result"
		tools = append(tools, advancedChatDeliveryTool(deliveryToolName))
	}
	systemPrompt := buildAdvancedChatCompletionSystemPrompt(prepared.agent, prepared.skills, prepared.workspaceSkills, prepared.mode)
	if agentGroupPrompt := advancedChatAgentGroupSystemPrompt(prepared.agentGroups); agentGroupPrompt != "" {
		if strings.TrimSpace(systemPrompt) == "" {
			systemPrompt = agentGroupPrompt
		} else {
			systemPrompt = strings.Join([]string{systemPrompt, agentGroupPrompt}, "\n\n")
		}
	}
	if connectorPrompt := advancedChatConnectorSystemPrompt(prepared.connectorDevice, prepared.connectorWorkspace); connectorPrompt != "" {
		if strings.TrimSpace(systemPrompt) == "" {
			systemPrompt = connectorPrompt
		} else {
			systemPrompt = strings.Join([]string{systemPrompt, connectorPrompt}, "\n\n")
		}
	}
	if prepared.delivery != nil {
		deliveryPrompt := "When the scheduled task is complete, call the deliver_result tool with a concise title and the final result body. Do this after you have produced the final result."
		if strings.TrimSpace(systemPrompt) == "" {
			systemPrompt = deliveryPrompt
		} else {
			systemPrompt = strings.Join([]string{systemPrompt, deliveryPrompt}, "\n\n")
		}
	}
	executorMessages := make([]ChatExecutorMessage, 0, len(prepared.messages)+prepared.maxToolRounds*2)
	for _, message := range prepared.messages {
		executorMessages = append(executorMessages, advancedChatExecutorMessage(user.ID, message))
	}

	totalCost := decimal.Zero
	totalToolCalls := 0
	toolCallDetails := []advancedChatCompletionToolCall{}
	contentParts := []advancedChatContentPart{}
	var lastContent string
	for round := 0; round < prepared.maxToolRounds; round++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if observer.OnStatus != nil {
			if err := observer.OnStatus(gin.H{"message": "model_round", "round": round + 1, "mode": prepared.mode}); err != nil {
				return nil, err
			}
		}
		streamedText := false
		request := ChatExecutorRequest{
			Context:         ctx,
			ModelName:       prepared.modelName,
			UserChannelID:   prepared.input.UserChannelID,
			Messages:        executorMessages,
			System:          systemPrompt,
			Tools:           tools,
			MaxTokens:       prepared.input.MaxTokens,
			Temperature:     prepared.input.Temperature,
			ReasoningEffort: normalizeAdvancedChatReasoningEffort(prepared.input.ReasoningEffort),
			Stream:          stream,
			OnTextDelta: func(delta string) error {
				if !stream || delta == "" || observer.OnText == nil {
					return nil
				}
				streamedText = true
				return observer.OnText(delta, round+1)
			},
		}
		result, err := executeAdvancedChatModelRequestWithRetry(ctx, user, request, observer, func() bool {
			return !streamedText
		})
		if err != nil {
			return nil, err
		}
		totalCost = totalCost.Add(result.Cost)
		lastContent = result.Content
		contentParts = appendAdvancedChatContentPart(contentParts, round+1, result.Content)
		if stream && !streamedText && strings.TrimSpace(result.Content) != "" && observer.OnText != nil {
			if err := observer.OnText(result.Content, round+1); err != nil {
				return nil, err
			}
		}
		if len(result.ToolCalls) == 0 {
			return &advancedChatCompletionResponse{
				Message:         advancedChatCompletionMessage{Role: "assistant", Content: result.Content, Parts: contentParts},
				Cost:            totalCost,
				ToolCalls:       totalToolCalls,
				ToolCallDetails: toolCallDetails,
			}, nil
		}

		totalToolCalls += len(result.ToolCalls)
		executorMessages = append(executorMessages, ChatExecutorMessage{
			Role:      "assistant",
			Content:   result.Content,
			ToolCalls: normalizeAssistantToolCalls(result.AssistantMessage),
		})
		for _, toolCall := range result.ToolCalls {
			binding, exists := bindings[toolCall.Name]
			connectorBinding, connectorExists := connectorBindings[toolCall.Name]
			deliveryExists := prepared.delivery != nil && toolCall.Name == deliveryToolName
			agentDelegateExists := toolCall.Name == advancedChatAgentDelegateToolName && len(prepared.agentGroups) > 0
			detail := advancedChatCompletionToolCall{ID: toolCall.ID, Round: round + 1, Name: toolCall.Name, Status: "running"}
			precreatedConnectorTaskID := ""
			var precreateConnectorTaskErr error
			if exists {
				detail.Server = binding.Server.Name
				detail.Tool = binding.Tool.Name
			} else if connectorExists {
				detail.Server = connectorBinding.DeviceName
				detail.Tool = connectorBinding.Action
			} else if deliveryExists {
				detail.Server = "result delivery"
				detail.Tool = "deliver_result"
			} else if agentDelegateExists {
				detail.Server = "agent group"
				detail.Tool = "agent_delegate"
			}
			arguments, argumentsErr := parseToolArguments(toolCall.Arguments)
			if argumentsErr == nil {
				if connectorExists {
					arguments = advancedChatConnectorToolPreviewArguments(ctx, user.ID, prepared.runID, connectorBinding, arguments)
					arguments = advancedChatConnectorArgumentsWithToolCallID(arguments, toolCall.ID)
				}
				detail.Arguments = arguments
			}
			if connectorExists && argumentsErr == nil && advancedChatConnectorTaskRequiresApproval(connectorBinding, arguments) {
				if connectorBinding.Action == "run_command" {
					task, err := createAdvancedChatConnectorTask(user.ID, prepared.runID, connectorBinding, arguments)
					if err != nil {
						precreateConnectorTaskErr = err
						detail.Status = "error"
					} else {
						precreatedConnectorTaskID = task.ID
						arguments = advancedChatConnectorArgumentsWithTaskID(arguments, task.ID)
						detail.Arguments = arguments
						detail.Status = "approval_required"
					}
				} else {
					detail.Status = "approval_required"
				}
			}
			if observer.OnToolCall != nil {
				if err := observer.OnToolCall(detail); err != nil {
					return nil, err
				}
			}
			detail.Status = "missing"
			toolResultText := "Tool not found: " + toolCall.Name
			if exists {
				detail.Server = binding.Server.Name
				detail.Tool = binding.Tool.Name
				if argumentsErr != nil {
					detail.Status = "invalid_arguments"
					toolResultText = "Invalid tool arguments: " + argumentsErr.Error()
				} else {
					toolResult, err := binding.Client.callTool(ctx, binding.Tool.Name, arguments)
					if err != nil {
						detail.Status = "error"
						toolResultText = "Tool call failed: " + err.Error()
					} else {
						detail.Status = "ok"
						toolResultText = toolResult.Text
						if toolResult.IsError {
							detail.Status = "error"
							toolResultText = "Tool returned an error: " + toolResultText
						}
					}
				}
			} else if connectorExists {
				detail.Server = connectorBinding.DeviceName
				detail.Tool = connectorBinding.Action
				if argumentsErr != nil {
					detail.Status = "invalid_arguments"
					toolResultText = "Invalid tool arguments: " + argumentsErr.Error()
				} else if precreateConnectorTaskErr != nil {
					detail.Status = "error"
					toolResultText = "Failed to create connector task: " + precreateConnectorTaskErr.Error()
				} else {
					var toolResult string
					var err error
					if precreatedConnectorTaskID != "" {
						toolResult, err = waitAdvancedChatConnectorTask(ctx, precreatedConnectorTaskID, user.ID)
					} else {
						toolResult, err = callAdvancedChatConnectorToolExpanded(ctx, user.ID, prepared.runID, connectorBinding, arguments)
					}
					if err != nil {
						detail.Status = "error"
						toolResultText = "Connector tool failed: " + err.Error()
						if strings.TrimSpace(toolResult) != "" {
							toolResultText = strings.TrimSpace(toolResult) + "\n\n" + toolResultText
						}
					} else {
						detail.Status = "ok"
						toolResultText = toolResult
					}
				}
			} else if deliveryExists {
				detail.Server = "result delivery"
				detail.Tool = "deliver_result"
				if argumentsErr != nil {
					detail.Status = "invalid_arguments"
					toolResultText = "Invalid delivery arguments: " + argumentsErr.Error()
				} else {
					toolResultText, err = deliverAdvancedChatResult(ctx, user.ID, prepared.delivery, arguments)
					if err != nil {
						detail.Status = "error"
						toolResultText = "Delivery failed: " + err.Error()
					} else {
						detail.Status = "ok"
					}
				}
			} else if agentDelegateExists {
				detail.Server = "agent group"
				detail.Tool = "agent_delegate"
				if argumentsErr != nil {
					detail.Status = "invalid_arguments"
					toolResultText = "Invalid delegation arguments: " + argumentsErr.Error()
				} else {
					toolResultText, err = executeAdvancedChatAgentDelegate(ctx, user, advancedChatAgentDelegateInput{
						UserID:             user.ID,
						RunID:              prepared.runID,
						ModelName:          prepared.modelName,
						UserChannelID:      prepared.input.UserChannelID,
						Messages:           executorMessages,
						WorkspaceSkills:    prepared.workspaceSkills,
						ConnectorDevice:    prepared.connectorDevice,
						ConnectorWorkspace: prepared.connectorWorkspace,
						ConnectorBindings:  connectorBindings,
						ConnectorTools:     connectorTools,
						Groups:             prepared.agentGroups,
						Arguments:          arguments,
					})
					if err != nil {
						detail.Status = "error"
						toolResultText = "Delegated agent failed: " + err.Error()
					} else {
						detail.Status = "ok"
					}
				}
			}
			detail.Result = truncateToolResult(toolResultText)
			toolCallDetails = append(toolCallDetails, detail)
			if observer.OnToolCall != nil {
				if err := observer.OnToolCall(detail); err != nil {
					return nil, err
				}
			}
			executorMessages = append(executorMessages, ChatExecutorMessage{
				Role:       "tool",
				Content:    truncateToolResult(toolResultText),
				ToolCallID: toolCall.ID,
				Name:       toolCall.Name,
			})
		}
	}
	return &advancedChatCompletionResponse{
		Message:         advancedChatCompletionMessage{Role: "assistant", Content: strings.TrimSpace(lastContent), Parts: contentParts},
		Cost:            totalCost,
		ToolCalls:       totalToolCalls,
		ToolCallDetails: toolCallDetails,
	}, nil
}

func executeAdvancedChatModelRequestWithRetry(
	ctx context.Context,
	user *model.User,
	request ChatExecutorRequest,
	observer advancedChatCompletionObserver,
	canRetry func() bool,
) (*ChatExecutorResult, error) {
	var lastErr error
	for attempt := 0; attempt <= assistantModelMaxRetries; attempt++ {
		result, err := ExecuteServerChatCompletion(nil, user, request)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if attempt == assistantModelMaxRetries || !retryableAdvancedChatModelRequestError(err) || (canRetry != nil && !canRetry()) {
			return nil, err
		}
		if observer.OnStatus != nil {
			if err := observer.OnStatus(gin.H{"message": "retrying", "attempt": attempt + 1, "max": assistantModelMaxRetries}); err != nil {
				return nil, err
			}
		}
		if err := sleepAdvancedChatModelRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func retryableAdvancedChatModelRequestError(err error) bool {
	var executorErr *ChatExecutorError
	if !errors.As(err, &executorErr) {
		return false
	}
	if executorErr.Status == http.StatusRequestTimeout || executorErr.Status == http.StatusTooManyRequests {
		return true
	}
	if executorErr.Status < http.StatusInternalServerError {
		return false
	}
	message := strings.TrimSpace(executorErr.Message)
	return message == "Upstream request failed" ||
		strings.HasPrefix(message, "Failed to read upstream") ||
		strings.HasPrefix(message, "Failed to parse upstream")
}

func sleepAdvancedChatModelRetry(ctx context.Context, attempt int) error {
	delay := advancedChatModelRetryDelay(attempt)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func advancedChatModelRetryDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := assistantModelRetryDelay
	for index := 0; index < attempt; index++ {
		delay *= 2
		if delay >= assistantModelRetryMaxDelay {
			return assistantModelRetryMaxDelay
		}
	}
	return delay
}

func finishAdvancedChatRun(runID string, sessionID string, userID uint, assistantMessageID string, response *advancedChatCompletionResponse) {
	content := strings.TrimSpace(response.Message.Content)
	contentParts, _ := json.Marshal(normalizeAdvancedChatContentParts(response.Message.Parts, content))
	toolDetailsJSON, _ := json.Marshal(response.ToolCallDetails)
	now := time.Now()
	finished := false
	_ = model.DB.Transaction(func(tx *gorm.DB) error {
		update := tx.Model(&AdvancedChatRun{}).
			Where("id = ? AND user_id = ? AND status IN ?", runID, userID, []string{advancedChatRunStatusQueued, advancedChatRunStatusRunning}).
			Updates(map[string]interface{}{
				"status":            advancedChatRunStatusCompleted,
				"status_message":    "completed",
				"error_message":     "",
				"cost":              response.Cost,
				"tool_calls":        response.ToolCalls,
				"tool_call_details": string(toolDetailsJSON),
				"finished_at":       &now,
			})
		if update.Error != nil || update.RowsAffected == 0 {
			return update.Error
		}
		if err := tx.Model(&AdvancedChatMessage{}).
			Where("id = ? AND user_id = ?", assistantMessageID, userID).
			Updates(map[string]interface{}{"content": content, "content_parts": string(contentParts), "tool_calls": string(toolDetailsJSON)}).Error; err != nil {
			return err
		}
		finished = true
		return nil
	})
	if finished {
		_ = appendAdvancedChatRunEvent(runID, sessionID, userID, "done", response)
	}
}

func failAdvancedChatRun(runID string, sessionID string, userID uint, assistantMessageID string, message string) {
	if strings.TrimSpace(message) == "" {
		message = "Assistant run failed"
	}
	now := time.Now()
	failed := false
	_ = model.DB.Transaction(func(tx *gorm.DB) error {
		update := tx.Model(&AdvancedChatRun{}).
			Where("id = ? AND user_id = ? AND status IN ?", runID, userID, []string{advancedChatRunStatusQueued, advancedChatRunStatusRunning}).
			Updates(map[string]interface{}{
				"status":         advancedChatRunStatusFailed,
				"status_message": "failed",
				"error_message":  message,
				"finished_at":    &now,
			})
		if update.Error != nil || update.RowsAffected == 0 {
			return update.Error
		}
		if err := tx.Model(&AdvancedChatMessage{}).
			Where("id = ? AND user_id = ? AND content = ?", assistantMessageID, userID, "").
			Update("content", message).Error; err != nil {
			return err
		}
		failed = true
		return nil
	})
	if failed {
		_ = appendAdvancedChatRunEvent(runID, sessionID, userID, "error", gin.H{"error": message})
	}
}

func appendAdvancedChatAssistantContent(messageID string, userID uint, delta string, round int) error {
	if delta == "" {
		return nil
	}
	if round <= 0 {
		round = 1
	}
	return model.DB.Transaction(func(tx *gorm.DB) error {
		var message AdvancedChatMessage
		if err := tx.Where("id = ? AND user_id = ?", messageID, userID).First(&message).Error; err != nil {
			return err
		}
		message.Content += delta
		parts := appendAdvancedChatContentPart(decodeAdvancedChatContentParts(message.ContentParts), round, delta)
		encoded, err := json.Marshal(parts)
		if err != nil {
			return err
		}
		return tx.Model(&message).Updates(map[string]interface{}{"content": message.Content, "content_parts": string(encoded)}).Error
	})
}

func createPersistedAdvancedChatCompletionSession(userID uint, input advancedChatCompletionInput, messages []advancedChatCompletionMessage, mode string, modelName string) (string, string, int, string, error) {
	sessionID := normalizeAdvancedChatSessionID(input.SessionID)
	if sessionID == "" {
		return "", "", http.StatusOK, "", nil
	}
	sessionMessages := make([]advancedChatSessionMessageInput, 0, len(messages))
	for _, message := range messages {
		sessionMessages = append(sessionMessages, advancedChatSessionMessageInput{
			ID:        message.ID,
			Role:      message.Role,
			Content:   message.Content,
			ToolCalls: message.ToolCalls,
		})
	}
	snapshot := advancedChatSessionInput{
		ID:                       sessionID,
		Title:                    input.Title,
		RunMode:                  mode,
		AgentID:                  input.AgentID,
		SkillIDs:                 input.SkillIDs,
		MCPServerIDs:             input.MCPServerIDs,
		ConnectorDeviceID:        input.ConnectorDeviceID,
		ConnectorWorkspacePath:   input.ConnectorWorkspacePath,
		ConnectorAutoApprove:     input.ConnectorAutoApprove,
		ConnectorCommandPrefixes: input.ConnectorCommandPrefixes,
		ModelName:                modelName,
		UserChannelID:            input.UserChannelID,
		MaxTokens:                normalizeAdvancedChatMaxTokens(input.MaxTokens),
		Temperature:              normalizeAdvancedChatTemperature(input.Temperature),
		ReasoningEffort:          normalizeAdvancedChatReasoningEffort(input.ReasoningEffort),
		Messages:                 sessionMessages,
	}
	if _, status, message, err := saveAdvancedChatSessionSnapshot(userID, sessionID, snapshot, true); err != nil {
		return "", "", status, message, err
	}
	assistantMessageID := newAdvancedChatID("acm")
	err := model.DB.Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		row := AdvancedChatMessage{
			ID:           assistantMessageID,
			SessionID:    sessionID,
			UserID:       userID,
			Role:         "assistant",
			Content:      "",
			ContentParts: "[]",
			ToolCalls:    "[]",
			SortOrder:    len(messages),
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		return tx.Model(&AdvancedChatSession{}).Where("id = ? AND user_id = ?", sessionID, userID).Update("updated_at", time.Now()).Error
	})
	if err != nil {
		return "", "", http.StatusInternalServerError, "Failed to save assistant message", err
	}
	return sessionID, assistantMessageID, http.StatusOK, "", nil
}

func finishPersistedAdvancedChatCompletionMessage(sessionID string, messageID string, userID uint, response advancedChatCompletionResponse) {
	if messageID == "" {
		return
	}
	toolCalls, _ := json.Marshal(response.ToolCallDetails)
	contentParts, _ := json.Marshal(normalizeAdvancedChatContentParts(response.Message.Parts, response.Message.Content))
	_ = model.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&AdvancedChatMessage{}).
			Where("id = ? AND user_id = ?", messageID, userID).
			Updates(map[string]interface{}{"content": response.Message.Content, "content_parts": string(contentParts), "tool_calls": string(toolCalls)}).Error; err != nil {
			return err
		}
		if sessionID == "" {
			return nil
		}
		return tx.Model(&AdvancedChatSession{}).Where("id = ? AND user_id = ?", sessionID, userID).Update("updated_at", time.Now()).Error
	})
}

func failPersistedAdvancedChatCompletionMessage(sessionID string, messageID string, userID uint, message string) {
	if messageID == "" || strings.TrimSpace(message) == "" {
		return
	}
	_ = model.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&AdvancedChatMessage{}).
			Where("id = ? AND user_id = ? AND content = ?", messageID, userID, "").
			Update("content", message).Error; err != nil {
			return err
		}
		if sessionID == "" {
			return nil
		}
		return tx.Model(&AdvancedChatSession{}).Where("id = ? AND user_id = ?", sessionID, userID).Update("updated_at", time.Now()).Error
	})
}

func mergeAdvancedChatMessageToolCall(messageID string, userID uint, detail advancedChatCompletionToolCall) error {
	if messageID == "" {
		return nil
	}
	return model.DB.Transaction(func(tx *gorm.DB) error {
		var message AdvancedChatMessage
		if err := tx.Where("id = ? AND user_id = ?", messageID, userID).First(&message).Error; err != nil {
			return err
		}
		details := decodeAdvancedChatToolCalls(message.ToolCalls)
		details = mergeAdvancedChatToolCallDetails(details, detail)
		encoded, err := json.Marshal(details)
		if err != nil {
			return err
		}
		return tx.Model(&message).Update("tool_calls", string(encoded)).Error
	})
}

func mergeAdvancedChatRunToolCall(runID string, userID uint, assistantMessageID string, detail advancedChatCompletionToolCall) error {
	return model.DB.Transaction(func(tx *gorm.DB) error {
		var run AdvancedChatRun
		if err := tx.Where("id = ? AND user_id = ?", runID, userID).First(&run).Error; err != nil {
			return err
		}
		details := decodeAdvancedChatToolCalls(run.ToolCallDetails)
		details = mergeAdvancedChatToolCallDetails(details, detail)
		encoded, err := json.Marshal(details)
		if err != nil {
			return err
		}
		if err := tx.Model(&run).Updates(map[string]interface{}{
			"tool_call_details": string(encoded),
			"tool_calls":        len(details),
		}).Error; err != nil {
			return err
		}
		return tx.Model(&AdvancedChatMessage{}).
			Where("id = ? AND user_id = ?", assistantMessageID, userID).
			Update("tool_calls", string(encoded)).Error
	})
}

func mergeAdvancedChatToolCallDetails(current []advancedChatCompletionToolCall, next advancedChatCompletionToolCall) []advancedChatCompletionToolCall {
	for index, item := range current {
		if item.ID != "" && item.ID == next.ID {
			current[index] = next
			return current
		}
		if item.ID == "" && next.ID == "" && item.Round == next.Round && item.Name == next.Name && item.Server == next.Server && item.Tool == next.Tool {
			current[index] = next
			return current
		}
	}
	return append(current, next)
}

func appendAdvancedChatRunEvent(runID string, sessionID string, userID uint, event string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var maxSeq int
	if err := model.DB.Model(&AdvancedChatRunEvent{}).
		Where("run_id = ? AND user_id = ?", runID, userID).
		Select("COALESCE(MAX(seq), 0)").
		Scan(&maxSeq).Error; err != nil {
		return err
	}
	row := AdvancedChatRunEvent{
		RunID:     runID,
		SessionID: sessionID,
		UserID:    userID,
		Seq:       maxSeq + 1,
		Event:     event,
		Payload:   string(data),
	}
	return model.DB.Create(&row).Error
}

func listAdvancedChatSessionResponses(userID uint) ([]advancedChatSessionResponse, error) {
	var sessions []AdvancedChatSession
	if err := model.DB.Where("user_id = ?", userID).Order("updated_at DESC").Limit(100).Find(&sessions).Error; err != nil {
		return nil, err
	}
	result := make([]advancedChatSessionResponse, 0, len(sessions))
	for _, session := range sessions {
		item, err := advancedChatSessionResponseFromModel(session)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, nil
}

func advancedChatSessionResponseFor(userID uint, sessionID string) (advancedChatSessionResponse, error) {
	var session AdvancedChatSession
	if err := model.DB.Where("id = ? AND user_id = ?", strings.TrimSpace(sessionID), userID).First(&session).Error; err != nil {
		return advancedChatSessionResponse{}, err
	}
	return advancedChatSessionResponseFromModel(session)
}

func advancedChatSessionResponseFromModel(session AdvancedChatSession) (advancedChatSessionResponse, error) {
	var messages []AdvancedChatMessage
	if err := model.DB.Where("session_id = ? AND user_id = ?", session.ID, session.UserID).Order("sort_order ASC, created_at ASC").Find(&messages).Error; err != nil {
		return advancedChatSessionResponse{}, err
	}
	messageResponses := make([]advancedChatMessageResponse, 0, len(messages))
	for _, message := range messages {
		messageResponses = append(messageResponses, advancedChatMessageResponseFromModel(message))
	}
	var latestRun *advancedChatRunResponse
	var run AdvancedChatRun
	if err := model.DB.Where("session_id = ? AND user_id = ?", session.ID, session.UserID).Order("created_at DESC").Limit(1).Find(&run).Error; err != nil {
		return advancedChatSessionResponse{}, err
	}
	if run.ID != "" {
		item := advancedChatRunResponseFromModel(run)
		latestRun = &item
	}
	return advancedChatSessionResponse{
		ID:                       session.ID,
		Title:                    session.Title,
		Messages:                 messageResponses,
		RunMode:                  normalizeAdvancedChatCompletionMode(session.RunMode),
		AgentID:                  session.AgentID,
		SkillIDs:                 decodeStringList(session.SkillIDs),
		MCPServerIDs:             decodeStringList(session.MCPServerIDs),
		ConnectorDeviceID:        session.ConnectorDeviceID,
		ConnectorWorkspacePath:   session.ConnectorWorkspacePath,
		ConnectorAutoApprove:     session.ConnectorAutoApprove,
		ConnectorCommandPrefixes: decodeStringList(session.ConnectorCommandPrefixes),
		ModelName:                session.ModelName,
		UserChannelID:            session.UserChannelID,
		MaxTokens:                session.MaxTokens,
		Temperature:              session.Temperature,
		ReasoningEffort:          normalizeAdvancedChatReasoningEffort(session.ReasoningEffort),
		LatestRun:                latestRun,
		CreatedAt:                session.CreatedAt,
		UpdatedAt:                session.UpdatedAt,
	}, nil
}

func advancedChatMessageResponseFromModel(message AdvancedChatMessage) advancedChatMessageResponse {
	return advancedChatMessageResponse{
		ID:        message.ID,
		Role:      message.Role,
		Content:   message.Content,
		Parts:     decodeAdvancedChatContentPartsWithFallback(message.ContentParts, message.Content),
		ToolCalls: decodeAdvancedChatToolCalls(message.ToolCalls),
		CreatedAt: message.CreatedAt,
		UpdatedAt: message.UpdatedAt,
	}
}

func advancedChatRunResponseFromModel(run AdvancedChatRun) advancedChatRunResponse {
	return advancedChatRunResponse{
		ID:                 run.ID,
		SessionID:          run.SessionID,
		AssistantMessageID: run.AssistantMessageID,
		Mode:               run.Mode,
		Status:             run.Status,
		StatusMessage:      run.StatusMessage,
		CurrentRound:       run.CurrentRound,
		ErrorMessage:       run.ErrorMessage,
		Cost:               run.Cost,
		ToolCalls:          run.ToolCalls,
		ToolCallDetails:    decodeAdvancedChatToolCalls(run.ToolCallDetails),
		StartedAt:          run.StartedAt,
		FinishedAt:         run.FinishedAt,
		CreatedAt:          run.CreatedAt,
		UpdatedAt:          run.UpdatedAt,
	}
}

func advancedChatRunEventResponseFromModel(event AdvancedChatRunEvent) advancedChatRunEventResponse {
	payload := map[string]interface{}{}
	if strings.TrimSpace(event.Payload) != "" {
		_ = json.Unmarshal([]byte(event.Payload), &payload)
	}
	return advancedChatRunEventResponse{
		ID:        event.ID,
		RunID:     event.RunID,
		SessionID: event.SessionID,
		Seq:       event.Seq,
		Event:     event.Event,
		Payload:   payload,
		CreatedAt: event.CreatedAt,
	}
}

func decodeAdvancedChatToolCalls(raw string) []advancedChatCompletionToolCall {
	if strings.TrimSpace(raw) == "" {
		return []advancedChatCompletionToolCall{}
	}
	var calls []advancedChatCompletionToolCall
	if err := json.Unmarshal([]byte(raw), &calls); err != nil || calls == nil {
		return []advancedChatCompletionToolCall{}
	}
	return calls
}

func decodeAdvancedChatContentPartsWithFallback(raw string, fallback string) []advancedChatContentPart {
	parts := decodeAdvancedChatContentParts(raw)
	if len(parts) > 0 {
		return parts
	}
	return normalizeAdvancedChatContentParts(nil, fallback)
}

func decodeAdvancedChatContentParts(raw string) []advancedChatContentPart {
	if strings.TrimSpace(raw) == "" {
		return []advancedChatContentPart{}
	}
	var parts []advancedChatContentPart
	if err := json.Unmarshal([]byte(raw), &parts); err != nil || parts == nil {
		return []advancedChatContentPart{}
	}
	return normalizeAdvancedChatContentParts(parts, "")
}

func normalizeAdvancedChatContentParts(parts []advancedChatContentPart, fallback string) []advancedChatContentPart {
	result := make([]advancedChatContentPart, 0, len(parts))
	for _, part := range parts {
		if part.Round <= 0 {
			part.Round = len(result) + 1
		}
		if strings.TrimSpace(part.Content) == "" {
			continue
		}
		result = append(result, part)
	}
	if len(result) == 0 && strings.TrimSpace(fallback) != "" {
		result = append(result, advancedChatContentPart{Round: 1, Content: fallback})
	}
	return result
}

func appendAdvancedChatContentPart(parts []advancedChatContentPart, round int, delta string) []advancedChatContentPart {
	if strings.TrimSpace(delta) == "" {
		return parts
	}
	if round <= 0 {
		round = 1
	}
	if len(parts) > 0 && parts[len(parts)-1].Round == round {
		parts[len(parts)-1].Content += delta
		return parts
	}
	return append(parts, advancedChatContentPart{Round: round, Content: delta})
}

func decodeStringList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return []string{}
	}
	var values []string
	if err := json.Unmarshal([]byte(raw), &values); err != nil || values == nil {
		return []string{}
	}
	return values
}

func normalizeAdvancedChatSessionID(raw string) string {
	id := strings.TrimSpace(raw)
	if id == "" || len(id) > 80 || strings.ContainsAny(id, `/\?#`) {
		return ""
	}
	return id
}

func advancedChatTitleFromMessages(messages []advancedChatCompletionMessage) string {
	for index := len(messages) - 1; index >= 0; index-- {
		if messages[index].Role != "user" {
			continue
		}
		title := strings.Join(strings.Fields(messages[index].Content), " ")
		if len([]rune(title)) > 28 {
			return string([]rune(title)[:28])
		}
		return title
	}
	return ""
}

func advancedChatTitleFromSessionMessages(messages []advancedChatSessionMessageInput) string {
	for index := len(messages) - 1; index >= 0; index-- {
		if strings.ToLower(strings.TrimSpace(messages[index].Role)) != "user" {
			continue
		}
		title := strings.Join(strings.Fields(messages[index].Content), " ")
		if len([]rune(title)) > 28 {
			return string([]rune(title)[:28])
		}
		return title
	}
	return ""
}

func newAdvancedChatID(prefix string) string {
	var raw [10]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return prefix + "-" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw[:]))
}

func normalizeAdvancedChatMaxTokens(value int) int {
	if value < 0 {
		return 0
	}
	if value > 200000 {
		return 200000
	}
	return value
}

func normalizeAdvancedChatTemperature(value *float64) *float64 {
	if value == nil {
		return nil
	}
	next := *value
	if next < 0 {
		next = 0
	}
	if next > 2 {
		next = 2
	}
	return &next
}

func normalizeAdvancedChatReasoningEffort(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "minimal":
		return "minimal"
	case "low":
		return "low"
	case "medium":
		return "medium"
	case "high":
		return "high"
	default:
		return ""
	}
}
