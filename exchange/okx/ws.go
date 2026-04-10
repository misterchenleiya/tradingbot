package okx

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/misterchenleiya/tradingbot/exchange/core"
	"github.com/misterchenleiya/tradingbot/exchange/transport"
)

const (
	wsOpcodeText  = 0x1
	wsOpcodeClose = 0x8
	wsOpcodePing  = 0x9
	wsOpcodePong  = 0xA
)

var errWSClosed = errors.New("websocket closed")

func (c *Client) GetTickerPriceWS(ctx context.Context, instID string) (float64, error) {
	return fetchTickerPriceWS(ctx, instID, c.dialer)
}

func fetchTickerPriceWS(ctx context.Context, instID string, dialer *transport.ProxyDialer) (price float64, err error) {
	ws, err := dialWebSocket(ctx, "wss://ws.okx.com:8443/ws/v5/public", dialer)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = core.CombineErrors(err, ws.Close())
	}()

	sub := fmt.Sprintf(`{"op":"subscribe","args":[{"channel":"tickers","instId":"%s"}]}`, instID)
	if err := ws.WriteText([]byte(sub)); err != nil {
		return 0, err
	}

	deadline, hasDeadline := ctx.Deadline()
	for {
		if hasDeadline {
			if err := ws.SetReadDeadline(deadline); err != nil {
				return 0, err
			}
		}
		msg, err := ws.ReadText()
		if err != nil {
			return 0, err
		}
		if msg == "pong" {
			continue
		}
		var resp okxWSTicker
		if err := json.Unmarshal([]byte(msg), &resp); err != nil {
			return 0, fmt.Errorf("ws decode failed: %w", err)
		}
		if resp.Event == "error" {
			return 0, fmt.Errorf("ws error: %s", resp.Msg)
		}
		if len(resp.Data) == 0 {
			continue
		}
		last := strings.TrimSpace(resp.Data[0].Last)
		if last == "" {
			continue
		}
		price, err = core.ParseFloat(last)
		if err != nil {
			return 0, err
		}
		return price, nil
	}
}

type okxWSTicker struct {
	Event string `json:"event"`
	Msg   string `json:"msg"`
	Data  []struct {
		Last string `json:"last"`
	} `json:"data"`
}

type wsConn struct {
	conn net.Conn
	r    *bufio.Reader
	mu   sync.Mutex
}

func dialWebSocket(ctx context.Context, urlStr string, dialer *transport.ProxyDialer) (*wsConn, error) {
	parsed, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "wss" {
		return nil, fmt.Errorf("unsupported scheme: %s", parsed.Scheme)
	}
	host := parsed.Hostname()
	port := parsed.Port()
	if port == "" {
		port = "443"
	}
	addr := net.JoinHostPort(host, port)

	dialFunc := (&net.Dialer{}).DialContext
	if dialer != nil {
		dialFunc = dialer.DialContext
	}
	rawConn, err := dialFunc(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		if err := rawConn.SetDeadline(deadline); err != nil {
			closeErr := rawConn.Close()
			return nil, core.CombineErrors(err, closeErr)
		}
	}
	conn := tls.Client(rawConn, &tls.Config{ServerName: host})
	if err := conn.Handshake(); err != nil {
		closeErr := rawConn.Close()
		return nil, core.CombineErrors(err, closeErr)
	}
	if hasDeadline {
		if err := rawConn.SetDeadline(time.Time{}); err != nil {
			closeErr := rawConn.Close()
			return nil, core.CombineErrors(err, closeErr)
		}
	}

	key, err := randomWebSocketKey()
	if err != nil {
		closeErr := conn.Close()
		return nil, core.CombineErrors(err, closeErr)
	}

	path := parsed.EscapedPath()
	if path == "" {
		path = "/"
	}
	if parsed.RawQuery != "" {
		path += "?" + parsed.RawQuery
	}

	req := strings.Builder{}
	req.WriteString("GET ")
	req.WriteString(path)
	req.WriteString(" HTTP/1.1\r\n")
	req.WriteString("Host: ")
	req.WriteString(parsed.Host)
	req.WriteString("\r\n")
	req.WriteString("Upgrade: websocket\r\n")
	req.WriteString("Connection: Upgrade\r\n")
	req.WriteString("Sec-WebSocket-Version: 13\r\n")
	req.WriteString("Sec-WebSocket-Key: ")
	req.WriteString(key)
	req.WriteString("\r\n")
	req.WriteString("User-Agent: gobot-trader\r\n")
	req.WriteString("\r\n")

	if _, err := conn.Write([]byte(req.String())); err != nil {
		closeErr := conn.Close()
		return nil, core.CombineErrors(err, closeErr)
	}

	reader := bufio.NewReader(conn)
	statusLine, err := readLine(reader)
	if err != nil {
		closeErr := conn.Close()
		return nil, core.CombineErrors(err, closeErr)
	}
	if !strings.Contains(statusLine, " 101 ") {
		closeErr := conn.Close()
		return nil, core.CombineErrors(fmt.Errorf("ws handshake failed: %s", statusLine), closeErr)
	}
	headers := make(map[string]string)
	for {
		line, err := readLine(reader)
		if err != nil {
			closeErr := conn.Close()
			return nil, core.CombineErrors(err, closeErr)
		}
		if line == "" {
			break
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(parts[0]))
		val := strings.TrimSpace(parts[1])
		headers[key] = val
	}
	accept := headers["sec-websocket-accept"]
	if accept == "" {
		closeErr := conn.Close()
		return nil, core.CombineErrors(errors.New("ws handshake missing accept"), closeErr)
	}
	expected, err := computeWebSocketAccept(key)
	if err != nil {
		closeErr := conn.Close()
		return nil, core.CombineErrors(err, closeErr)
	}
	if accept != expected {
		closeErr := conn.Close()
		return nil, core.CombineErrors(errors.New("ws handshake invalid accept"), closeErr)
	}

	return &wsConn{conn: conn, r: reader}, nil
}

func randomWebSocketKey() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buf), nil
}

func computeWebSocketAccept(key string) (string, error) {
	h := sha1.New()
	if _, err := io.WriteString(h, key+"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	return line, nil
}

func (c *wsConn) Close() error {
	writeErr := c.writeFrame(wsOpcodeClose, []byte{})
	closeErr := c.conn.Close()
	return core.CombineErrors(writeErr, closeErr)
}

func (c *wsConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *wsConn) WriteText(payload []byte) error {
	return c.writeFrame(wsOpcodeText, payload)
}

func (c *wsConn) ReadText() (string, error) {
	for {
		opcode, payload, err := c.readFrame()
		if err != nil {
			return "", err
		}
		switch opcode {
		case wsOpcodeText:
			return string(payload), nil
		case wsOpcodePing:
			if err := c.writeFrame(wsOpcodePong, payload); err != nil {
				return "", err
			}
		case wsOpcodePong:
			continue
		case wsOpcodeClose:
			return "", errWSClosed
		default:
			continue
		}
	}
}

func (c *wsConn) readFrame() (byte, []byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(c.r, header); err != nil {
		return 0, nil, err
	}
	opcode := header[0] & 0x0f
	masked := (header[1] & 0x80) != 0
	length := int(header[1] & 0x7f)
	if length == 126 {
		ext := make([]byte, 2)
		if _, err := io.ReadFull(c.r, ext); err != nil {
			return 0, nil, err
		}
		length = int(binary.BigEndian.Uint16(ext))
	} else if length == 127 {
		ext := make([]byte, 8)
		if _, err := io.ReadFull(c.r, ext); err != nil {
			return 0, nil, err
		}
		val := binary.BigEndian.Uint64(ext)
		if val > math.MaxInt32 {
			return 0, nil, fmt.Errorf("ws frame too large: %d", val)
		}
		length = int(val)
	}

	var maskKey []byte
	if masked {
		maskKey = make([]byte, 4)
		if _, err := io.ReadFull(c.r, maskKey); err != nil {
			return 0, nil, err
		}
	}

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(c.r, payload); err != nil {
			return 0, nil, err
		}
	}
	if masked {
		for i := 0; i < len(payload); i++ {
			payload[i] ^= maskKey[i%4]
		}
	}
	return opcode, payload, nil
}

func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	length := len(payload)
	if length > math.MaxInt32 {
		return fmt.Errorf("ws payload too large: %d", length)
	}
	maskKey := make([]byte, 4)
	if _, err := rand.Read(maskKey); err != nil {
		return err
	}
	maskedPayload := make([]byte, length)
	copy(maskedPayload, payload)
	for i := 0; i < length; i++ {
		maskedPayload[i] ^= maskKey[i%4]
	}

	var header []byte
	if length < 126 {
		header = []byte{0x80 | opcode, 0x80 | byte(length)}
	} else if length <= 0xffff {
		header = []byte{0x80 | opcode, 0x80 | 126, 0, 0}
		binary.BigEndian.PutUint16(header[2:], uint16(length))
	} else {
		header = []byte{0x80 | opcode, 0x80 | 127, 0, 0, 0, 0, 0, 0, 0, 0}
		binary.BigEndian.PutUint64(header[2:], uint64(length))
	}

	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	if _, err := c.conn.Write(maskKey); err != nil {
		return err
	}
	if length > 0 {
		if _, err := c.conn.Write(maskedPayload); err != nil {
			return err
		}
	}
	return nil
}
