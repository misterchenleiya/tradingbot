package okx

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/common/control"
	"github.com/misterchenleiya/tradingbot/exchange"
	"github.com/misterchenleiya/tradingbot/exchange/config"
	"github.com/misterchenleiya/tradingbot/exchange/core"
	"github.com/misterchenleiya/tradingbot/exchange/transport"
)

const baseURL = "https://www.okx.com"

type Client struct {
	apiKey     string
	secretKey  string
	passphrase string
	baseURL    string
	client     *http.Client
	dialer     *transport.ProxyDialer
	controller *control.Controller
	simulated  bool
}

func New(cfg config.ExchangeConfig) (*Client, error) {
	dialer, err := transport.NewProxyDialer(cfg.Proxy)
	if err != nil {
		return nil, err
	}
	return &Client{
		apiKey:     cfg.APIKey,
		secretKey:  cfg.SecretKey,
		passphrase: cfg.Passphrase,
		baseURL:    baseURL,
		client:     newHTTPClient(dialer),
		dialer:     dialer,
		controller: newRateController(cfg),
		simulated:  cfg.Simulated,
	}, nil
}

func (c *Client) Name() string {
	return "okx"
}

func (c *Client) NormalizeSymbol(raw string) (string, error) {
	text := strings.ToUpper(strings.TrimSpace(raw))
	if text == "" {
		return "", errors.New("symbol is required")
	}
	if strings.HasSuffix(text, "-SWAP") || strings.Contains(text, ":") {
		return "", fmt.Errorf("invalid symbol format: %s (use TradingView format like BTC/USDT.P)", raw)
	}
	if !strings.HasSuffix(text, ".P") {
		return "", fmt.Errorf("invalid symbol format: %s (use TradingView format like BTC/USDT.P)", raw)
	}
	pairText := strings.TrimSuffix(text, ".P")
	pair := strings.Split(pairText, "/")
	if len(pair) != 2 || pair[0] == "" || pair[1] == "" {
		return "", fmt.Errorf("invalid symbol pair: %s", raw)
	}
	if pair[1] != "USDT" {
		return "", fmt.Errorf("only USDT quote is supported: %s", pair[1])
	}
	return fmt.Sprintf("%s-%s-SWAP", pair[0], pair[1]), nil
}

func newHTTPClient(dialer *transport.ProxyDialer) *http.Client {
	if dialer == nil {
		return &http.Client{Timeout: 10 * time.Second}
	}
	transport := &http.Transport{
		DialContext: dialer.DialContext,
	}
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}
}

func (c *Client) GetPositionMode(ctx context.Context) (string, error) {
	data, err := c.doJSON(ctx, http.MethodGet, "/api/v5/account/config", nil, nil, true)
	if err != nil {
		return "", err
	}
	var rows []struct {
		PosMode string `json:"posMode"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", errors.New("empty account config")
	}
	return rows[0].PosMode, nil
}

func (c *Client) SetPositionMode(ctx context.Context, mode string) error {
	payload := map[string]string{"posMode": mode}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	data, err := c.doJSON(ctx, http.MethodPost, "/api/v5/account/set-position-mode", nil, body, true)
	if err != nil {
		return err
	}
	var rows []okxExecResult
	if err := json.Unmarshal(data, &rows); err != nil {
		return err
	}
	for _, row := range rows {
		if strings.TrimSpace(row.SCode) == "" {
			continue
		}
		if row.SCode != "0" {
			return okxAPIError(http.MethodPost, "/api/v5/account/set-position-mode", row.SCode, row.SMsg)
		}
	}
	return nil
}

func (c *Client) SetLeverage(ctx context.Context, instID, marginMode string, leverage int, posSide string) error {
	payload := map[string]string{
		"instId":  instID,
		"mgnMode": marginMode,
		"lever":   strconv.Itoa(leverage),
	}
	if posSide != "" {
		payload["posSide"] = posSide
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	data, err := c.doJSON(ctx, http.MethodPost, "/api/v5/account/set-leverage", nil, body, true)
	if err != nil {
		return err
	}
	var rows []okxExecResult
	if err := json.Unmarshal(data, &rows); err != nil {
		return err
	}
	return ensureOKXSuccess(http.MethodPost, "/api/v5/account/set-leverage", rows)
}

func (c *Client) GetInstrument(ctx context.Context, instID string) (exchange.Instrument, error) {
	query := url.Values{}
	query.Set("instType", "SWAP")
	query.Set("instId", instID)
	data, err := c.doJSON(ctx, http.MethodGet, "/api/v5/public/instruments", query, nil, false)
	if err != nil {
		return exchange.Instrument{}, err
	}
	var rows []struct {
		InstID string `json:"instId"`
		CtVal  string `json:"ctVal"`
		LotSz  string `json:"lotSz"`
		TickSz string `json:"tickSz"`
		MinSz  string `json:"minSz"`
		State  string `json:"state"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return exchange.Instrument{}, err
	}
	if len(rows) == 0 {
		return exchange.Instrument{}, fmt.Errorf("instrument not found: %s", instID)
	}
	item := rows[0]
	ctVal, err := core.ParseFloat(item.CtVal)
	if err != nil {
		return exchange.Instrument{}, fmt.Errorf("invalid ctVal: %w", err)
	}
	lotSz, err := core.ParseFloat(item.LotSz)
	if err != nil {
		return exchange.Instrument{}, fmt.Errorf("invalid lotSz: %w", err)
	}
	tickSz, err := core.ParseFloat(item.TickSz)
	if err != nil {
		return exchange.Instrument{}, fmt.Errorf("invalid tickSz: %w", err)
	}
	minSz := 0.0
	if strings.TrimSpace(item.MinSz) != "" {
		minSz, err = core.ParseFloat(item.MinSz)
		if err != nil {
			return exchange.Instrument{}, fmt.Errorf("invalid minSz: %w", err)
		}
	}
	return exchange.Instrument{
		InstID: instID,
		CtVal:  ctVal,
		LotSz:  lotSz,
		TickSz: tickSz,
		MinSz:  minSz,
		State:  item.State,
	}, nil
}

func (c *Client) GetTickerPrice(ctx context.Context, instID string) (float64, error) {
	query := url.Values{}
	query.Set("instId", instID)
	data, err := c.doJSON(ctx, http.MethodGet, "/api/v5/market/ticker", query, nil, false)
	if err != nil {
		return 0, err
	}
	var rows []struct {
		Last string `json:"last"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, errors.New("empty ticker")
	}
	return core.ParseFloat(rows[0].Last)
}

func (c *Client) GetPositions(ctx context.Context, instID string) ([]exchange.Position, error) {
	query := url.Values{}
	query.Set("instType", "SWAP")
	if instID != "" {
		query.Set("instId", instID)
	}
	data, err := c.doJSON(ctx, http.MethodGet, "/api/v5/account/positions", query, nil, true)
	if err != nil {
		return nil, err
	}
	var rows []exchange.Position
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *Client) GetPositionsHistory(ctx context.Context, instID string) ([]exchange.PositionHistory, error) {
	query := url.Values{}
	query.Set("instType", "SWAP")
	if instID != "" {
		query.Set("instId", instID)
	}
	data, err := c.doJSON(ctx, http.MethodGet, "/api/v5/account/positions-history", query, nil, true)
	if err != nil {
		return nil, err
	}
	var rows []exchange.PositionHistory
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *Client) GetOpenTPSLOrders(ctx context.Context, instID string) ([]exchange.TPSLOrder, error) {
	ordTypes := []string{"oco", "conditional", "move_order_stop"}
	var rows []exchange.TPSLOrder
	for _, ordType := range ordTypes {
		subset, err := c.getOpenTPSLOrdersByType(ctx, ordType, instID)
		if err != nil {
			return nil, err
		}
		rows = append(rows, subset...)
	}
	return rows, nil
}

func (c *Client) SupportsAttachAlgoOrders() bool {
	return true
}

func (c *Client) getOpenTPSLOrdersByType(ctx context.Context, ordType, instID string) ([]exchange.TPSLOrder, error) {
	query := url.Values{}
	query.Set("instType", "SWAP")
	query.Set("ordType", ordType)
	if instID != "" {
		query.Set("instId", instID)
	}
	data, err := c.doJSON(ctx, http.MethodGet, "/api/v5/trade/orders-algo-pending", query, nil, true)
	if err != nil {
		return nil, err
	}
	var rows []exchange.TPSLOrder
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *Client) CancelTPSLOrders(ctx context.Context, reqs []exchange.CancelTPSLOrderRequest) error {
	if len(reqs) == 0 {
		return nil
	}
	payload := make([]map[string]string, 0, len(reqs))
	for _, req := range reqs {
		payload = append(payload, map[string]string{
			"algoId": req.OrderID,
			"instId": req.InstID,
		})
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	data, err := c.doJSON(ctx, http.MethodPost, "/api/v5/trade/cancel-algos", nil, body, true)
	if err != nil {
		return err
	}
	var rows []okxExecResult
	if err := json.Unmarshal(data, &rows); err != nil {
		return err
	}
	return ensureOKXSuccess(http.MethodPost, "/api/v5/trade/cancel-algos", rows)
}

func (c *Client) PlaceTPSLOrder(ctx context.Context, req exchange.TPSLOrderRequest) (string, error) {
	ordType := "conditional"
	if req.TPTriggerPx != "" && req.SLTriggerPx != "" {
		ordType = "oco"
	}
	payload := map[string]any{
		"instId":  req.InstID,
		"tdMode":  req.TdMode,
		"side":    req.Side,
		"ordType": ordType,
	}
	if req.PosSide != "" {
		payload["posSide"] = req.PosSide
	}
	if req.Sz != "" {
		payload["sz"] = req.Sz
	}
	if req.ReduceOnly {
		payload["reduceOnly"] = true
	}
	if req.ClientOrderID != "" {
		payload["algoClOrdId"] = req.ClientOrderID
	}
	if req.TPTriggerPx != "" {
		payload["tpTriggerPx"] = req.TPTriggerPx
		payload["tpOrdPx"] = "-1"
	}
	if req.SLTriggerPx != "" {
		payload["slTriggerPx"] = req.SLTriggerPx
		payload["slOrdPx"] = "-1"
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	data, err := c.doJSON(ctx, http.MethodPost, "/api/v5/trade/order-algo", nil, body, true)
	if err != nil {
		return "", err
	}
	var rows []okxAlgoOrderResponse
	if err := json.Unmarshal(data, &rows); err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", errors.New("empty algo order response")
	}
	if err := ensureOKXAlgoOrderSuccess(http.MethodPost, "/api/v5/trade/order-algo", rows[0]); err != nil {
		return "", err
	}
	return rows[0].AlgoID, nil
}

func (c *Client) GetBalance(ctx context.Context) (exchange.BalanceSnapshot, error) {
	trading, err := c.getTradingBalance(ctx)
	if err != nil {
		return exchange.BalanceSnapshot{}, err
	}
	funding, err := c.getFundingBalance(ctx)
	if err != nil {
		return exchange.BalanceSnapshot{}, err
	}
	return exchange.BalanceSnapshot{
		Trading: trading,
		Funding: funding,
	}, nil
}

func (c *Client) getTradingBalance(ctx context.Context) ([]exchange.Balance, error) {
	query := url.Values{}
	query.Set("ccy", "USDT")
	data, err := c.doJSON(ctx, http.MethodGet, "/api/v5/account/balance", query, nil, true)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		Details []exchange.Balance `json:"details"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("empty trading balance")
	}
	return rows[0].Details, nil
}

func (c *Client) getFundingBalance(ctx context.Context) ([]exchange.Balance, error) {
	query := url.Values{}
	query.Set("ccy", "USDT")
	data, err := c.doJSON(ctx, http.MethodGet, "/api/v5/asset/balances", query, nil, true)
	if err != nil {
		return nil, err
	}
	var rows []exchange.Balance
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *Client) PlaceOrder(ctx context.Context, req exchange.OrderRequest) (string, error) {
	payload := map[string]any{
		"instId":  req.InstID,
		"tdMode":  req.TdMode,
		"side":    req.Side,
		"ordType": req.OrdType,
		"sz":      req.Sz,
	}
	if req.PosSide != "" {
		payload["posSide"] = req.PosSide
	}
	if req.Px != "" {
		payload["px"] = req.Px
	}
	if req.ReduceOnly {
		payload["reduceOnly"] = true
	}
	if req.ClientOrderID != "" {
		payload["clOrdId"] = req.ClientOrderID
	}
	if len(req.AttachAlgoOrds) > 0 {
		payload["attachAlgoOrds"] = req.AttachAlgoOrds
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	data, err := c.doJSON(ctx, http.MethodPost, "/api/v5/trade/order", nil, body, true)
	if err != nil {
		return "", err
	}
	var rows []okxOrderResponse
	if err := json.Unmarshal(data, &rows); err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", errors.New("empty order response")
	}
	if err := ensureOKXOrderSuccess(http.MethodPost, "/api/v5/trade/order", rows[0]); err != nil {
		return "", err
	}
	return rows[0].OrdID, nil
}

func (c *Client) GetOrder(ctx context.Context, instID, ordID string) (exchange.Order, error) {
	query := url.Values{}
	query.Set("instId", instID)
	query.Set("ordId", ordID)
	data, err := c.doJSON(ctx, http.MethodGet, "/api/v5/trade/order", query, nil, true)
	if err != nil {
		return exchange.Order{}, err
	}
	var rows []exchange.Order
	if err := json.Unmarshal(data, &rows); err != nil {
		return exchange.Order{}, err
	}
	if len(rows) == 0 {
		return exchange.Order{}, errors.New("empty order detail")
	}
	return rows[0], nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, query url.Values, body []byte, auth bool) ([]byte, error) {
	endpoint := okxEndpointForPath(method, path)
	if c.controller == nil {
		return c.doJSONOnce(ctx, method, path, query, body, auth)
	}
	var out []byte
	err := c.controller.Do(ctx, control.Meta{
		Scope:    c.Name(),
		Endpoint: endpoint,
	}, func(execCtx context.Context) error {
		data, err := c.doJSONOnce(execCtx, method, path, query, body, auth)
		if err != nil {
			return err
		}
		out = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) doJSONOnce(ctx context.Context, method, path string, query url.Values, body []byte, auth bool) ([]byte, error) {
	requestPath := path
	if len(query) > 0 {
		requestPath = requestPath + "?" + query.Encode()
	}
	fullURL := c.baseURL + requestPath

	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, bodyReader)
	if err != nil {
		return nil, err
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.simulated {
		req.Header.Set("x-simulated-trading", "1")
	}
	if auth {
		timestamp := okxTimestamp()
		signature, err := okxSign(c.secretKey, timestamp, method, requestPath, body)
		if err != nil {
			return nil, err
		}
		req.Header.Set("OK-ACCESS-KEY", c.apiKey)
		req.Header.Set("OK-ACCESS-SIGN", signature)
		req.Header.Set("OK-ACCESS-TIMESTAMP", timestamp)
		req.Header.Set("OK-ACCESS-PASSPHRASE", c.passphrase)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			return nil, core.CombineErrors(fmt.Errorf("http status %s", resp.Status), closeErr)
		}
		return nil, core.CombineErrors(fmt.Errorf("http status %s: %s", resp.Status, strings.TrimSpace(string(bodyBytes))), closeErr)
	}

	respBytes, err := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if err != nil {
		return nil, core.CombineErrors(err, closeErr)
	}
	if closeErr != nil {
		return nil, closeErr
	}
	var wrapper okxResponse
	if err := json.Unmarshal(respBytes, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Code != "0" {
		return nil, okxAPIErrorWithData(method, requestPath, wrapper.Code, wrapper.Msg, wrapper.Data)
	}
	return wrapper.Data, nil
}

func okxTimestamp() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}

func okxAPIError(method, path, code, msg string) error {
	return okxAPIErrorWithData(method, path, code, msg, nil)
}

func okxAPIErrorWithData(method, path, code, msg string, data json.RawMessage) error {
	code = strings.TrimSpace(code)
	msg = strings.TrimSpace(msg)
	if code == "" {
		code = "unknown"
	}
	if msg == "" {
		msg = "unknown"
	}
	dataText := strings.TrimSpace(string(data))
	if dataText != "" && dataText != "null" && dataText != "[]" {
		return fmt.Errorf("okx error %s %s: code=%s msg=%s data=%s", method, path, code, msg, dataText)
	}
	return fmt.Errorf("okx error %s %s: code=%s msg=%s", method, path, code, msg)
}

func okxSign(secret, timestamp, method, requestPath string, body []byte) (string, error) {
	prehash := timestamp + strings.ToUpper(method) + requestPath + string(body)
	mac := hmac.New(sha256.New, []byte(secret))
	if _, err := mac.Write([]byte(prehash)); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

type okxResponse struct {
	Code string          `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type okxExecResult struct {
	SCode string `json:"sCode"`
	SMsg  string `json:"sMsg"`
}

type okxOrderResponse struct {
	OrdID   string `json:"ordId"`
	SCode   string `json:"sCode"`
	SMsg    string `json:"sMsg"`
	ClOrdID string `json:"clOrdId"`
}

type okxAlgoOrderResponse struct {
	AlgoID  string `json:"algoId"`
	SCode   string `json:"sCode"`
	SMsg    string `json:"sMsg"`
	ClOrdID string `json:"algoClOrdId"`
}

func ensureOKXSuccess(method, path string, rows []okxExecResult) error {
	for _, row := range rows {
		if strings.TrimSpace(row.SCode) == "" {
			continue
		}
		if row.SCode != "0" {
			return okxAPIError(method, path, row.SCode, row.SMsg)
		}
	}
	return nil
}

func ensureOKXOrderSuccess(method, path string, row okxOrderResponse) error {
	if row.SCode != "0" {
		return okxAPIError(method, path, row.SCode, row.SMsg)
	}
	if row.OrdID == "" {
		return errors.New("okx order missing ordId")
	}
	return nil
}

func ensureOKXAlgoOrderSuccess(method, path string, row okxAlgoOrderResponse) error {
	if row.SCode != "0" {
		return okxAPIError(method, path, row.SCode, row.SMsg)
	}
	if row.AlgoID == "" {
		return errors.New("okx algo order missing algoId")
	}
	return nil
}
