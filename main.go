package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"ntfy/pkg/ntfy"
)

const (
	requestTopic  = "uclient-req"
	responseTopic = "uclient-res"
)

func runResponder(ctx context.Context) {
	go func() {
		client := ntfy.GetClient()
		err := client.Serve(ctx, requestTopic, func(_ context.Context, m *ntfy.Message) (string, string, bool, error) {
			fmt.Printf("[responder] 收到: %s tags=%v\n", m.Message, m.Tags)
			reply := fmt.Sprintf("ack: %s (at %s)", m.Message, time.Now().Format(time.RFC3339))
			return "reply", reply, false, nil
		})
		if err != nil {
			fmt.Println("异常退出", err)
		}
	}()
}

func main() {
	//ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var ops []ntfy.Option
	if value := getenv("NTFY_ADDRESS", "https://ntfy.sh"); value != "" {
		ops = append(ops, ntfy.WithServer(value))
	}
	if admin, password := getenv("NTFY_USERNAME", "admin"), getenv("NTFY_PASSWORD", "password"); admin != "" && password != "" {
		ops = append(ops, ntfy.WithBasicAuth(admin, password))
	}
	ops = append(ops, ntfy.WithRetry(ntfy.RetryConfig{
		MaxAttempts:    5,
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
		Multiplier:     2.0,
	}))

	client := ntfy.GetClient(ops...)
	if os.Getenv("RESPONDER") == "1" {
		fmt.Println("以响应方模式运行，订阅:", requestTopic)
		runResponder(ctx)
		<-ctx.Done()
		return
	}

	runResponder(ctx)
	time.Sleep(500 * time.Millisecond)

	msg := "hello, please reply"
	fmt.Printf("[sender] PublishAndWait 同步调用: %s\n", msg)
	reply, err := client.PublishAndWait(ctx, requestTopic, responseTopic, "request", msg, 15*time.Second)
	if err != nil {
		fmt.Println("错误❌:", err)
		os.Exit(1)
	}
	fmt.Printf("[sender] 收到响应: %s tags=%v\n", reply.Message, reply.Tags)
	fmt.Printf("[sender] singleton check: %p == %p -> %v\n", client, ntfy.GetClient(), client == ntfy.GetClient())
	<-ctx.Done()
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
