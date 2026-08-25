package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cschleiden/go-workflows/workflow"
)

type OrderInput struct {
	CustomerID string
	Amount     float64
}

func OrderWorkflow(ctx workflow.Context, input OrderInput) (string, error) {
	logger := workflow.Logger(ctx)
	logger.Info("Processing order", "customer", input.CustomerID, "amount", input.Amount)

	// Step 1: Validate
	valid, err := workflow.ExecuteActivity[bool](ctx, workflow.DefaultActivityOptions, ValidateOrder, input).Get(ctx)
	if err != nil {
		return "", fmt.Errorf("validation failed: %w", err)
	}
	if !valid {
		return "", fmt.Errorf("order invalid")
	}

	// Step 2: Charge payment
	txnID, err := workflow.ExecuteActivity[string](ctx, workflow.DefaultActivityOptions, ChargePayment, input.CustomerID, input.Amount).Get(ctx)
	if err != nil {
		return "", fmt.Errorf("payment failed: %w", err)
	}

	// Step 3: Send confirmation
	_, err = workflow.ExecuteActivity[string](ctx, workflow.DefaultActivityOptions, SendConfirmation, input.CustomerID, txnID).Get(ctx)
	if err != nil {
		return "", fmt.Errorf("confirmation failed: %w", err)
	}

	return fmt.Sprintf("order completed: txn=%s", txnID), nil
}

func ValidateOrder(ctx context.Context, input OrderInput) (bool, error) {
	if input.Amount <= 0 {
		return false, fmt.Errorf("invalid amount: %f", input.Amount)
	}
	if input.CustomerID == "" {
		return false, fmt.Errorf("missing customer ID")
	}
	// Simulate validation check
	time.Sleep(100 * time.Millisecond)
	return true, nil
}

func ChargePayment(ctx context.Context, customerID string, amount float64) (string, error) {
	// Simulate payment processing
	time.Sleep(250 * time.Millisecond)
	txnID := fmt.Sprintf("txn-%s-%.0f", customerID, amount)
	return txnID, nil
}

func SendConfirmation(ctx context.Context, customerID string, txnID string) (string, error) {
	// Simulate sending confirmation email
	time.Sleep(50 * time.Millisecond)
	fmt.Printf("  → Confirmation sent to %s for transaction %s\n", customerID, txnID)
	return "sent", nil
}
