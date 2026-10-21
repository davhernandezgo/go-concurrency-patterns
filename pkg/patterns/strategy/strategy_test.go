package strategy

import (
	"context"
	"testing"
)

func TestPaymentStrategy_RuntimeSwitching(t *testing.T) {
	ctx := context.Background()

	cc := &CreditCardStrategy{CardNumber: "4111222233334567", CVV: "123"}
	crypto := &CryptoStrategy{WalletAddress: "0x71C845175574b885D75ca02721047E79bE37DA1f", Network: "ETH"}

	paymentCtx := NewPaymentContext(cc)

	// Pay with Credit Card
	res1, err := paymentCtx.ExecutePayment(ctx, 150.0)
	if err != nil {
		t.Fatalf("unexpected CC payment error: %v", err)
	}
	if res1.Method != "CREDIT_CARD" || res1.Status != "SETTLED" {
		t.Fatalf("unexpected CC result: %+v", res1)
	}

	// Hot-swap strategy to Crypto
	paymentCtx.SetStrategy(crypto)

	res2, err := paymentCtx.ExecutePayment(ctx, 300.0)
	if err != nil {
		t.Fatalf("unexpected Crypto payment error: %v", err)
	}
	if res2.Method != "CRYPTO" || res2.Status != "CONFIRMED_ON_CHAIN" {
		t.Fatalf("unexpected Crypto result: %+v", res2)
	}
}

func TestPaymentStrategy_InvalidAmount(t *testing.T) {
	paymentCtx := NewPaymentContext(&CreditCardStrategy{CardNumber: "1234567812345678", CVV: "999"})
	_, err := paymentCtx.ExecutePayment(context.Background(), -10.0)
	if err == nil {
		t.Fatal("expected error for negative amount, got nil")
	}
}
