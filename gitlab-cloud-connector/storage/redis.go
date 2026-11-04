package storage

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RedisClient defines the operations needed by PulseSync/GitLab Connector.
type RedisClient interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, val string) error
	LPush(ctx context.Context, key, val string) error
	LRange(ctx context.Context, key string, start, stop int) ([]string, error)
	LTrim(ctx context.Context, key string, start, stop int) error
	Del(ctx context.Context, key string) error
	IsExternal() bool
}

// NewClient connects to an external Redis instance (e.g. "localhost:6379" or Kubernetes service).
// If Redis is unreachable, it automatically provides an embedded in-memory Redis engine
// so the connector runs anywhere with zero friction.
func NewClient(addr string) RedisClient {
	if addr == "" {
		addr = "localhost:6379"
	}

	// Try connecting to real Redis
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err == nil {
		_ = conn.Close()
		fmt.Printf("[REDIS] Successfully connected to external Redis at %s\n", addr)
		return &tcpRedisClient{addr: addr}
	}

	fmt.Printf("[REDIS] No external Redis at %s (%v)\n", addr, err)
	fmt.Printf("[REDIS] -> Activating embedded in-memory Redis engine (automatic zero-setup fallback).\n")
	return newInMemoryRedis()
}

// =========================================================================
// Native Go RESP (REdis Serialization Protocol) Client
// =========================================================================

type tcpRedisClient struct {
	addr string
	mu   sync.Mutex
}

func (c *tcpRedisClient) IsExternal() bool { return true }

func (c *tcpRedisClient) exec(args ...string) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := net.DialTimeout("tcp", c.addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Write RESP Command
	var cmd strings.Builder
	cmd.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		cmd.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}

	if _, err := conn.Write([]byte(cmd.String())); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(conn)
	return c.readRESP(reader)
}

func (c *tcpRedisClient) readRESP(r *bufio.Reader) (any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if len(line) == 0 {
		return nil, fmt.Errorf("empty RESP line")
	}

	switch line[0] {
	case '+': // Simple string
		return line[1:], nil
	case '-': // Error
		return nil, fmt.Errorf("redis error: %s", line[1:])
	case ':': // Integer
		return strconv.ParseInt(line[1:], 10, 64)
	case '$': // Bulk string
		length, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if length == -1 {
			return "", nil
		}
		buf := make([]byte, length+2)
		var readTotal int
		for readTotal < length+2 {
			n, err := r.Read(buf[readTotal:])
			if err != nil {
				return nil, err
			}
			readTotal += n
		}
		return string(buf[:length]), nil
	case '*': // Array
		count, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if count == -1 {
			return nil, nil
		}
		arr := make([]string, 0, count)
		for i := 0; i < count; i++ {
			elem, err := c.readRESP(r)
			if err != nil {
				return nil, err
			}
			if s, ok := elem.(string); ok {
				arr = append(arr, s)
			}
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("unknown RESP byte: %c", line[0])
	}
}

func (c *tcpRedisClient) Get(_ context.Context, key string) (string, error) {
	res, err := c.exec("GET", key)
	if err != nil {
		return "", err
	}
	if s, ok := res.(string); ok {
		return s, nil
	}
	return "", nil
}

func (c *tcpRedisClient) Set(_ context.Context, key, val string) error {
	_, err := c.exec("SET", key, val)
	return err
}

func (c *tcpRedisClient) LPush(_ context.Context, key, val string) error {
	_, err := c.exec("LPUSH", key, val)
	return err
}

func (c *tcpRedisClient) LRange(_ context.Context, key string, start, stop int) ([]string, error) {
	res, err := c.exec("LRANGE", key, strconv.Itoa(start), strconv.Itoa(stop))
	if err != nil {
		return nil, err
	}
	if arr, ok := res.([]string); ok {
		return arr, nil
	}
	return []string{}, nil
}

func (c *tcpRedisClient) LTrim(_ context.Context, key string, start, stop int) error {
	_, err := c.exec("LTRIM", key, strconv.Itoa(start), strconv.Itoa(stop))
	return err
}

func (c *tcpRedisClient) Del(_ context.Context, key string) error {
	_, err := c.exec("DEL", key)
	return err
}

// =========================================================================
// In-Memory Fallback Engine (for Local Dev & Testing)
// =========================================================================

type inMemoryRedis struct {
	mu      sync.RWMutex
	kvStore map[string]string
	lists   map[string][]string
}

func newInMemoryRedis() *inMemoryRedis {
	return &inMemoryRedis{
		kvStore: make(map[string]string),
		lists:   make(map[string][]string),
	}
}

func (m *inMemoryRedis) IsExternal() bool { return false }

func (m *inMemoryRedis) Get(_ context.Context, key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.kvStore[key], nil
}

func (m *inMemoryRedis) Set(_ context.Context, key, val string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kvStore[key] = val
	return nil
}

func (m *inMemoryRedis) LPush(_ context.Context, key, val string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lists[key] = append([]string{val}, m.lists[key]...)
	return nil
}

func (m *inMemoryRedis) LRange(_ context.Context, key string, start, stop int) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := m.lists[key]
	if len(list) == 0 {
		return []string{}, nil
	}
	if start < 0 {
		start = 0
	}
	if start >= len(list) {
		return []string{}, nil
	}
	if stop < 0 || stop >= len(list) {
		stop = len(list) - 1
	}
	res := make([]string, stop-start+1)
	copy(res, list[start:stop+1])
	return res, nil
}

func (m *inMemoryRedis) LTrim(_ context.Context, key string, start, stop int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	list := m.lists[key]
	if len(list) == 0 {
		return nil
	}
	if stop >= len(list) {
		stop = len(list) - 1
	}
	if start <= stop && start < len(list) {
		m.lists[key] = list[start : stop+1]
	}
	return nil
}

func (m *inMemoryRedis) Del(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.kvStore, key)
	delete(m.lists, key)
	return nil
}
