package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/he141142/solar-shared/domain"
)

// Exchange là cổng kết nối tới sàn giao dịch điện.
type Exchange interface {
	Place(ctx context.Context, in domain.TradeIntent) (domain.OrderResult, error)
}

// mtlsSecret là nội dung secret trong Secrets Manager (inject qua env EXCHANGE_MTLS).
type mtlsSecret struct {
	CertPem string `json:"certPem"`
	KeyPem  string `json:"keyPem"`
	CaPem   string `json:"caPem"`
	APIKey  string `json:"apiKey"`
}

func newExchange(mode, endpoint, secretJSON string) (Exchange, error) {
	if mode != "live" {
		return mockExchange{}, nil
	}

	var s mtlsSecret
	if err := json.Unmarshal([]byte(secretJSON), &s); err != nil {
		return nil, fmt.Errorf("decode exchange secret: %w", err)
	}
	if s.CertPem == "" || s.KeyPem == "" {
		return nil, errors.New("exchange secret has no client certificate")
	}
	cert, err := tls.X509KeyPair([]byte(s.CertPem), []byte(s.KeyPem))
	if err != nil {
		return nil, fmt.Errorf("load client cert: %w", err)
	}
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if s.CaPem != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(s.CaPem)) {
			return nil, errors.New("invalid CA bundle")
		}
		tlsCfg.RootCAs = pool
	}
	return &httpExchange{
		endpoint: endpoint,
		apiKey:   s.APIKey,
		client: &http.Client{
			Timeout:   15 * time.Second,
			Transport: &http.Transport{TLSClientConfig: tlsCfg},
		},
	}, nil
}

type httpExchange struct {
	endpoint string
	apiKey   string
	client   *http.Client
}

func (h *httpExchange) Place(ctx context.Context, in domain.TradeIntent) (domain.OrderResult, error) {
	body, err := json.Marshal(map[string]any{
		"clientOrderId":    in.IntentID, // sàn dùng để chống đặt trùng
		"side":             in.Side,
		"quantityKwh":      in.QuantityKwh,
		"limitPrice":       in.LimitPrice,
		"deliveryInterval": in.Interval,
		"zone":             in.Zone,
	})
	if err != nil {
		return domain.OrderResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint+"/orders", bytes.NewReader(body))
	if err != nil {
		return domain.OrderResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", h.apiKey)

	resp, err := h.client.Do(req)
	if err != nil {
		return domain.OrderResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return domain.OrderResult{}, fmt.Errorf("exchange returned %s", resp.Status)
	}
	var res domain.OrderResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return domain.OrderResult{}, fmt.Errorf("decode exchange response: %w", err)
	}
	res.IntentID = in.IntentID
	return res, nil
}

// mockExchange giả lập giá thị trường 1500-3500 VND/kWh, khớp nếu giá thoả limit.
type mockExchange struct{}

func (mockExchange) Place(_ context.Context, in domain.TradeIntent) (domain.OrderResult, error) {
	market := math.Round(1500 + rand.Float64()*2000)
	filled := (in.Side == "BUY" && market <= in.LimitPrice) || (in.Side == "SELL" && market >= in.LimitPrice)

	res := domain.OrderResult{
		IntentID: in.IntentID,
		OrderID:  fmt.Sprintf("MOCK-%d", time.Now().UnixNano()),
		Status:   "REJECTED",
		Price:    market,
	}
	if filled {
		res.Status, res.FilledKwh = "FILLED", in.QuantityKwh
	}
	return res, nil
}
