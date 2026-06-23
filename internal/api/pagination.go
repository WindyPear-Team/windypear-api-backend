package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const (
	defaultPageSize = 20
	maxPageSize     = 100
)

type paginatedResponse struct {
	Items    interface{} `json:"items"`
	Total    int64       `json:"total"`
	Page     int         `json:"page"`
	PageSize int         `json:"page_size"`
}

func wantsPaginatedResponse(c *gin.Context) bool {
	for _, key := range []string{"page", "page_size", "start_time", "end_time", "start_date", "end_date", "api_key_id"} {
		if strings.TrimSpace(c.Query(key)) != "" {
			return true
		}
	}
	return false
}

func parsePagination(c *gin.Context) (int, int) {
	page := positiveIntQuery(c, "page", 1)
	pageSize := positiveIntQuery(c, "page_size", defaultPageSize)
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	return page, pageSize
}

func positiveIntQuery(c *gin.Context, key string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(c.Query(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func applyCreatedAtRange(query *gorm.DB, c *gin.Context, column string) (*gorm.DB, error) {
	if startRaw := firstNonEmptyString(c.Query("start_time"), c.Query("start_date")); strings.TrimSpace(startRaw) != "" {
		start, _, err := parseTimeBoundary(startRaw, false)
		if err != nil {
			return query, err
		}
		query = query.Where(column+" >= ?", start)
	}
	if endRaw := firstNonEmptyString(c.Query("end_time"), c.Query("end_date")); strings.TrimSpace(endRaw) != "" {
		end, exclusive, err := parseTimeBoundary(endRaw, true)
		if err != nil {
			return query, err
		}
		if exclusive {
			query = query.Where(column+" < ?", end)
		} else {
			query = query.Where(column+" <= ?", end)
		}
	}
	return query, nil
}

func applyDateStringRange(query *gorm.DB, c *gin.Context, column string) (*gorm.DB, error) {
	if startRaw := firstNonEmptyString(c.Query("start_date"), c.Query("start_time")); strings.TrimSpace(startRaw) != "" {
		start, err := parseDateString(startRaw)
		if err != nil {
			return query, err
		}
		query = query.Where(column+" >= ?", start)
	}
	if endRaw := firstNonEmptyString(c.Query("end_date"), c.Query("end_time")); strings.TrimSpace(endRaw) != "" {
		end, err := parseDateString(endRaw)
		if err != nil {
			return query, err
		}
		query = query.Where(column+" <= ?", end)
	}
	return query, nil
}

func parseTimeBoundary(raw string, end bool) (time.Time, bool, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return time.Time{}, false, errors.New("empty time")
	}
	if date, err := time.ParseInLocation("2006-01-02", value, time.Local); err == nil {
		if end {
			return date.AddDate(0, 0, 1), true, nil
		}
		return date, false, nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02T15:04"} {
		if parsed, err := time.ParseInLocation(layout, value, time.Local); err == nil {
			return parsed, false, nil
		}
	}
	return time.Time{}, false, errors.New("invalid time format")
}

func parseDateString(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if len(value) >= len("2006-01-02") {
		value = value[:len("2006-01-02")]
	}
	if _, err := time.ParseInLocation("2006-01-02", value, time.Local); err != nil {
		return "", errors.New("invalid date format")
	}
	return value, nil
}

func writePaginationError(c *gin.Context, err error) bool {
	if err == nil {
		return false
	}
	c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	return true
}
