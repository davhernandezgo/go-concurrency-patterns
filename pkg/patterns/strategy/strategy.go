package strategy

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrUnsupportedStrategy = errors.New("unsupported payment strategy")
	ErrInvalidAmount       = errors.New("payment amount must be positive")
)

// PaymentResult contains execution status and transaction receipt.
type PaymentResult struct {
	TransactionID string
	Method        string
	Amount        float64
	Status        string
}

// PaymentStrategy defines the interface that all payment algorithms must satisfy.
type PaymentStrategy interface {
	Pay(ctx context.Context, amount float64) (*PaymentResult, error)
	Name() string
}

// CreditCardStrategy processes credit card payments.
type CreditCardStrategy struct {
	CardNumber string
	CVV        string
}

func (c *CreditCardStrategy) Name() string { return "CREDIT_CARD" }
func (c *CreditCardStrategy) Pay(ctx context.Context, amount float64) (*PaymentResult, error) {
	if amount <= 0 {
		return nil, ErrInvalidAmount
	}
	return &PaymentResult{
		TransactionID: fmt.Sprintf("cc_%s_%d", c.CardNumber[len(c.CardNumber)-4:], int(amount)),
		Method:        c.Name(),
		Amount:        amount,
		Status:        "SETTLED",
	}, nil
}

// CryptoStrategy processes cryptocurrency payments.
type CryptoStrategy struct {
	WalletAddress string
	Network       string
}

func (k *CryptoStrategy) Name() string { return "CRYPTO" }
func (k *CryptoStrategy) Pay(ctx context.Context, amount float64) (*PaymentResult, error) {
	if amount <= 0 {
		return nil, ErrInvalidAmount
	}
	return &PaymentResult{
		TransactionID: fmt.Sprintf("0x%s_%s", k.Network, k.WalletAddress[:6]),
		Method:        k.Name(),
		Amount:        amount,
		Status:        "CONFIRMED_ON_CHAIN",
	}, nil
}

// PaymentContext orchestrates payments using the configured strategy.
type PaymentContext struct {
	strategy PaymentStrategy
}

// NewPaymentContext creates context with an initial strategy.
func NewPaymentContext(strategy PaymentStrategy) *PaymentContext {
	return &PaymentContext{strategy: strategy}
}

// SetStrategy allows hot-swapping the payment strategy at runtime.
func (p *PaymentContext) SetStrategy(strategy PaymentStrategy) {
	p.strategy = strategy
}

// ExecutePayment processes the payment using the current strategy.
func (p *PaymentContext) ExecutePayment(ctx context.Context, amount float64) (*PaymentResult, error) {
	if p.strategy == nil {
		return nil, ErrUnsupportedStrategy
	}
	return p.strategy.Pay(ctx, amount)
}
