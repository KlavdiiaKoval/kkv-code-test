package rwclient

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Client struct {
	QueueURL   string
	QueueName  string
	HttpClient *http.Client
}

func New(queueURL, queueName string) *Client {
	return &Client{
		QueueURL:   queueURL,
		QueueName:  queueName,
		HttpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) Produce(ctx context.Context, inputPath string) error {
	f, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			if len(line) > 0 {
				// last line without newline — still enqueue
				if err := c.enqueue(ctx, line); err != nil {
					return err
				}
			}
			return nil
		}
		if err != nil {
			return err
		}
		if err := c.enqueue(ctx, line); err != nil {
			return err
		}
	}
}

func (c *Client) Consume(ctx context.Context, outputPath string) error {
	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		msg, err := c.dequeue(ctx)
		if err != nil || len(msg) == 0 {
			continue
		}
		if _, err := f.Write(msg); err != nil {
			return err
		}
	}
}

func (c *Client) enqueue(ctx context.Context, body []byte) error {
	url := fmt.Sprintf("%s/queues/%s", c.QueueURL, c.QueueName)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.HttpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("enqueue failed: %s: %s", resp.Status, string(b))
	}
	return nil
}

func (c *Client) dequeue(ctx context.Context) ([]byte, error) {
	url := fmt.Sprintf("%s/queues/%s", c.QueueURL, c.QueueName)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return b, nil
	case http.StatusNoContent:
		return nil, nil
	default:
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("dequeue failed: %s: %s", resp.Status, string(b))
	}
}

func (c *Client) QueueLength(ctx context.Context) (int, error) {
	url := fmt.Sprintf("%s/queues/%s", c.QueueURL, c.QueueName)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.HttpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("queue length failed: %s: %s", resp.Status, string(b))
	}
	h := resp.Header.Get("X-Queue-Len")
	if h == "" {
		return 0, fmt.Errorf("missing X-Queue-Len header")
	}
	n, err := strconv.Atoi(h)
	if err != nil {
		return 0, fmt.Errorf("invalid X-Queue-Len header: %w", err)
	}
	return n, nil
}
