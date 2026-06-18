package service

import (
	"errors"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

var ErrInsufficientBalance = errors.New("insufficient balance")

type StartupHook func() error
type RouteHook func(*gin.RouterGroup)
type UsageChargeHook func(tx *gorm.DB, userID uint, cost decimal.Decimal) error

var startupHooks []StartupHook
var adminRouteHooks []RouteHook
var userRouteHooks []RouteHook
var usageChargeHook UsageChargeHook

func RegisterStartupHook(hook StartupHook) {
	startupHooks = append(startupHooks, hook)
}

func RunStartupHooks() error {
	for _, hook := range startupHooks {
		if hook == nil {
			continue
		}
		if err := hook(); err != nil {
			return err
		}
	}
	return nil
}

func RegisterAdminRouteHook(hook RouteHook) {
	adminRouteHooks = append(adminRouteHooks, hook)
}

func RegisterUserRouteHook(hook RouteHook) {
	userRouteHooks = append(userRouteHooks, hook)
}

func ApplyAdminRouteHooks(group *gin.RouterGroup) {
	for _, hook := range adminRouteHooks {
		if hook != nil {
			hook(group)
		}
	}
}

func ApplyUserRouteHooks(group *gin.RouterGroup) {
	for _, hook := range userRouteHooks {
		if hook != nil {
			hook(group)
		}
	}
}

func RegisterUsageChargeHook(hook UsageChargeHook) {
	usageChargeHook = hook
}

func ApplyUsageCharge(tx *gorm.DB, userID uint, cost decimal.Decimal) error {
	if cost.LessThanOrEqual(decimal.Zero) {
		return nil
	}
	if usageChargeHook != nil {
		return usageChargeHook(tx, userID, cost)
	}
	balanceUpdate := tx.Exec("UPDATE users SET balance = balance - ? WHERE id = ? AND balance >= ?", cost, userID, cost)
	if balanceUpdate.Error != nil {
		return balanceUpdate.Error
	}
	if balanceUpdate.RowsAffected == 0 {
		return ErrInsufficientBalance
	}
	return nil
}
