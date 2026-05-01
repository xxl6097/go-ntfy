package ntfy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Message 表示 ntfy /json 流中的一条事件。
type Message struct {
	ID      string   `json:"id"`
	Time    int64    `json:"time"`
	Event   string   `json:"event"`
	Topic   string   `json:"topic"`
	Message string   `json:"message"`
	Title   string   `json:"title"`
	Tags    []string `json:"tags"`
}

// RetryConfig 控制订阅失败时的重试策略。
// MaxAttempts <= 0 表示无限重试（直到 ctx 取消）。
type RetryConfig struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
}

// Client 封装 ntfy 的发送与订阅。
type Client struct {
	server     string
	httpClient *http.Client
	username   string
	password   string
	token      string
	retry      RetryConfig
}

type Option func(*Client)

func WithServer(url string) Option {
	return func(c *Client) { c.server = strings.TrimRight(url, "/") }
}

func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.httpClient = hc }
}

func WithBasicAuth(username, password string) Option {
	return func(c *Client) {
		c.username = username
		c.password = password
	}
}

func WithToken(token string) Option {
	return func(c *Client) { c.token = token }
}

// WithRetry 自定义订阅失败重试策略。
// 新增：
//
// RetryConfig{ MaxAttempts, InitialBackoff, MaxBackoff, Multiplier } —— MaxAttempts<=0 表示无限重试。
// WithRetry(rc) Option，单例默认值：无限次、初始 1s、最大 30s、倍率 2x、±20% 抖动。
// 内部 connectStream 单次连接 + connectStreamWithRetry 带退避重连，统一供 Subscribe 与 PublishAndWait 使用。
// 行为：
//
// Subscribe：连接失败、流断开（EOF/读错误）都会自动重连，仅当 handler 返回 false 或 ctx 取消时退出。
// PublishAndWait：只在"建立订阅"阶段重试；一旦请求已发送，仅在剩余 timeout 内等待匹配响应（避免重发请求导致重复处理）。
// 退避遵循 ctx，取消会立即返回 ctx.Err()。
func WithRetry(rc RetryConfig) Option {
	return func(c *Client) { c.retry = rc }
}

func (c *Client) applyAuth(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
		return
	}
	if c.username != "" || c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}
}

var (
	instance *Client
	initOnce sync.Once
)

func GetClient(opts ...Option) *Client {
	initOnce.Do(func() {
		c := &Client{
			server:     "https://ntfy.sh",
			httpClient: &http.Client{Timeout: 0},
			retry: RetryConfig{
				MaxAttempts:    0, // 无限
				InitialBackoff: 1 * time.Second,
				MaxBackoff:     30 * time.Second,
				Multiplier:     2.0,
			},
		}
		for _, opt := range opts {
			opt(c)
		}
		instance = c
	})
	return instance
}

func newCorrelationID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Publish 发送一条消息，不等待响应。
func (c *Client) Publish(ctx context.Context, topic, title, msg string, tags []string) error {
	url := fmt.Sprintf("%s/%s", c.server, topic)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString(msg))
	if err != nil {
		return err
	}
	if title != "" {
		req.Header.Set("Title", title)
	}
	if len(tags) > 0 {
		req.Header.Set("X-Tags", strings.Join(tags, ","))
	}
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("publish failed: %s: %s", resp.Status, string(body))
	}
	return nil
}

// streamConn 表示一次已建立的订阅连接。
type streamConn struct {
	resp    *http.Response
	scanner *bufio.Scanner
}

func (s *streamConn) Close() {
	if s.resp != nil {
		s.resp.Body.Close()
	}
}

// connectStream 单次建立订阅连接（不带重试）。
func (c *Client) connectStream(ctx context.Context, topic string) (*streamConn, error) {
	url := fmt.Sprintf("%s/%s/json", c.server, topic)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	c.applyAuth(req)
	fmt.Printf("do connect %+v\n", req.URL.String())
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("subscribe failed: %s: %s", resp.Status, string(body))
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	return &streamConn{resp: resp, scanner: scanner}, nil
}

// connectStreamWithRetry 按 retry 配置进行带退避的重连。
func (c *Client) connectStreamWithRetry(ctx context.Context, topic string, onRetry func(attempt int, err error, next time.Duration)) (*streamConn, error) {
	return c.connectStreamWithRetryConfig(ctx, topic, c.retry, onRetry)
}

// connectStreamWithRetryConfig 使用指定 retry 配置进行带退避的重连。
func (c *Client) connectStreamWithRetryConfig(ctx context.Context, topic string, rc RetryConfig, onRetry func(attempt int, err error, next time.Duration)) (*streamConn, error) {
	if rc.InitialBackoff <= 0 {
		rc.InitialBackoff = 1 * time.Second
	}
	if rc.MaxBackoff <= 0 {
		rc.MaxBackoff = 30 * time.Second
	}
	if rc.Multiplier <= 1 {
		rc.Multiplier = 2.0
	}

	backoff := rc.InitialBackoff
	for attempt := 1; ; attempt++ {
		conn, err := c.connectStream(ctx, topic)
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if rc.MaxAttempts > 0 && attempt >= rc.MaxAttempts {
			return nil, fmt.Errorf("subscribe %s: gave up after %d attempts: %w", topic, attempt, err)
		}
		// jitter: ±20%
		jitter := time.Duration(float64(backoff) * (0.8 + 0.4*mrand.Float64()))
		if onRetry != nil {
			onRetry(attempt, err, jitter)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(jitter):
		}
		backoff = time.Duration(float64(backoff) * rc.Multiplier)
		if backoff > rc.MaxBackoff {
			backoff = rc.MaxBackoff
		}
	}
}

// PublishAndWait 同步发送 reqTopic 并阻塞等待 respTopic 上匹配 correlation ID 的响应。
// 订阅建立阶段失败会按 retry 配置重试；发送之后不再重连，仅在 timeout 内等待响应。
func (c *Client) PublishAndWait(ctx context.Context, reqTopic, respTopic, title, msg string, timeout time.Duration) (*Message, error) {
	cid := newCorrelationID()

	subCtx, subCancel := context.WithTimeout(ctx, timeout)
	defer subCancel()

	conn, err := c.connectStreamWithRetry(subCtx, respTopic, func(attempt int, err error, next time.Duration) {
		// 默认静默；调用方如需日志可包装 httpClient.Transport
	})
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if !waitForOpen(conn.scanner) {
		return nil, fmt.Errorf("subscribe stream closed before open")
	}

	pubURL := fmt.Sprintf("%s/%s", c.server, reqTopic)
	pubReq, err := http.NewRequestWithContext(ctx, http.MethodPost, pubURL, bytes.NewBufferString(msg))
	if err != nil {
		return nil, err
	}
	if title != "" {
		pubReq.Header.Set("Title", title)
	}
	pubReq.Header.Set("X-Tags", "cid="+cid+",replyto="+respTopic)
	pubReq.Header.Set("X-Reply-To", respTopic)
	c.applyAuth(pubReq)
	pubResp, err := c.httpClient.Do(pubReq)
	if err != nil {
		return nil, err
	}
	io.Copy(io.Discard, pubResp.Body)
	pubResp.Body.Close()
	if pubResp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("publish failed: %s", pubResp.Status)
	}

	for conn.scanner.Scan() {
		line := conn.scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var m Message
		if err := json.Unmarshal(line, &m); err != nil {
			continue
		}
		if m.Event != "message" {
			continue
		}
		if hasTag(m.Tags, "cid="+cid) {
			return &m, nil
		}
	}
	if err := conn.scanner.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("no matching response (cid=%s)", cid)
}

// Subscribe 订阅 topic，逐条调用 handler；连接失败或流断开时按 retry 配置自动重连。
// handler 返回 false 即终止订阅；ctx 取消时返回 ctx.Err()。
func (c *Client) Subscribe(ctx context.Context, topic string, handler func(*Message) bool) error {
	// 流断开后重连不受 MaxAttempts 限制：只要 ctx 未取消就一直重试。
	rc := c.retry
	rc.MaxAttempts = 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		conn, err := c.connectStreamWithRetryConfig(ctx, topic, rc, func(attempt int, err error, next time.Duration) {
			fmt.Printf("subscribe %s reconnect attempt=%d after=%s err=%v\n", topic, attempt, next, err)
		})
		if err != nil {
			return err
		}
		stop := false
		for conn.scanner.Scan() {
			var m Message
			if err := json.Unmarshal(conn.scanner.Bytes(), &m); err != nil {
				continue
			}
			if m.Event != "message" {
				continue
			}
			if !handler(&m) {
				stop = true
				break
			}
		}
		scanErr := conn.scanner.Err()
		conn.Close()
		if stop {
			return nil
		}
		// 流被关闭（EOF 或读错误），重连
		if ctx.Err() != nil {
			fmt.Println("流被关闭 ctx.Err()", ctx.Err())
			return ctx.Err()
		}
		fmt.Printf("subscribe %s stream closed err=%v, reconnecting\n", topic, scanErr)
		// 短暂歇口气再走重试逻辑
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.retry.InitialBackoff):
		}
	}
}

func waitForOpen(scanner *bufio.Scanner) bool {
	for scanner.Scan() {
		var m Message
		if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
			continue
		}
		if m.Event == "open" || m.Event == "message" {
			return true
		}
	}
	return false
}

func hasTag(tags []string, want string) bool {
	for _, t := range tags {
		if t == want {
			return true
		}
	}
	return false
}

// Handler 处理收到的请求，返回回复的 title/body。
// 返回 err != nil 时，错误信息会作为 body 自动回发，并加上 tag "error=1"。
// 返回 (skip=true) 时不回复（适合该消息不需要响应的情况）。
type Handler func(ctx context.Context, m *Message) (title string, body string, skip bool, err error)

// Serve 订阅 topic 并自动回复请求方。
// 仅处理由 PublishAndWait 发出的"RPC 风格"消息（必须带 cid= 与 replyto= 两个标签）；
// 其它消息会被忽略，避免误回。连接失败按 retry 配置自动重连。
func (c *Client) Serve(ctx context.Context, topic string, h Handler) error {
	return c.Subscribe(ctx, topic, func(m *Message) bool {
		var cidTag, replyTo string
		for _, t := range m.Tags {
			switch {
			case strings.HasPrefix(t, "cid="):
				cidTag = t
			case strings.HasPrefix(t, "replyto="):
				replyTo = strings.TrimPrefix(t, "replyto=")
			}
		}
		if cidTag == "" || replyTo == "" {
			return true // 非 RPC 消息，跳过
		}

		title, body, skip, err := h(ctx, m)
		if skip {
			return true
		}
		tags := []string{cidTag}
		if err != nil {
			body = err.Error()
			tags = append(tags, "error=1")
		}
		_ = c.Publish(ctx, replyTo, title, body, tags)
		return true
	})
}
