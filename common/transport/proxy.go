package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	socks5Version = 0x05
	socks5NoAuth  = 0x00
	socks5CmdConn = 0x01
	socks5ATypV4  = 0x01
	socks5ATypDNS = 0x03
	socks5ATypV6  = 0x04
)

var ErrSocks5AuthUnsupported = errors.New("socks5 auth not supported")

type ProxyDialer struct {
	proxyAddr string
	direct    net.Dialer
}

func NewProxyDialer(raw string) (*ProxyDialer, error) {
	addr := strings.TrimSpace(raw)
	if addr == "" {
		return nil, nil
	}
	normalized, err := CanonicalProxyAddress(addr)
	if err != nil {
		return nil, err
	}
	return &ProxyDialer{proxyAddr: normalized}, nil
}

func CanonicalProxyAddress(raw string) (string, error) {
	addr := strings.TrimSpace(raw)
	if addr == "" {
		return "", nil
	}
	normalized, err := normalizeProxyAddress(addr)
	if err != nil {
		return "", err
	}
	host, port, err := net.SplitHostPort(normalized)
	if err != nil {
		return "", fmt.Errorf("invalid proxy address: %s", normalized)
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return "", fmt.Errorf("invalid proxy address: %s", normalized)
	}
	portNum, err := strconv.Atoi(strings.TrimSpace(port))
	if err != nil || portNum <= 0 || portNum > 65535 {
		return "", fmt.Errorf("invalid proxy address: %s", normalized)
	}
	return net.JoinHostPort(host, strconv.Itoa(portNum)), nil
}

func normalizeProxyAddress(raw string) (string, error) {
	if strings.HasPrefix(raw, "socks5://") || strings.HasPrefix(raw, "socks5h://") {
		parsed, err := url.Parse(raw)
		if err != nil {
			return "", err
		}
		if parsed.User != nil {
			return "", ErrSocks5AuthUnsupported
		}
		if parsed.Host == "" {
			return "", fmt.Errorf("invalid proxy address: %s", raw)
		}
		return parsed.Host, nil
	}
	return raw, nil
}

func (d *ProxyDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if d == nil || d.proxyAddr == "" {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
	return dialSocks5(ctx, d.direct, d.proxyAddr, addr)
}

func dialSocks5(ctx context.Context, dialer net.Dialer, proxyAddr, targetAddr string) (net.Conn, error) {
	conn, err := dialer.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, err
	}
	deadline, ok := ctx.Deadline()
	if ok {
		if err := conn.SetDeadline(deadline); err != nil {
			closeErr := conn.Close()
			return nil, combineErrors(err, closeErr)
		}
	}
	if err := socks5Handshake(conn, targetAddr); err != nil {
		closeErr := conn.Close()
		return nil, combineErrors(err, closeErr)
	}
	if ok {
		if err := conn.SetDeadline(time.Time{}); err != nil {
			closeErr := conn.Close()
			return nil, combineErrors(err, closeErr)
		}
	}
	return conn, nil
}

func socks5Handshake(conn net.Conn, targetAddr string) error {
	if err := writeAll(conn, []byte{socks5Version, 0x01, socks5NoAuth}); err != nil {
		return err
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}
	if resp[0] != socks5Version {
		return fmt.Errorf("socks5 invalid version: %d", resp[0])
	}
	if resp[1] != socks5NoAuth {
		return ErrSocks5AuthUnsupported
	}

	host, portStr, err := net.SplitHostPort(targetAddr)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return err
	}
	if port <= 0 || port > 65535 {
		return fmt.Errorf("invalid target port: %d", port)
	}

	addrType, addrBytes, err := encodeSocks5Addr(host)
	if err != nil {
		return err
	}
	req := make([]byte, 0, 4+len(addrBytes)+2)
	req = append(req, socks5Version, socks5CmdConn, 0x00, addrType)
	req = append(req, addrBytes...)
	portBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(portBuf, uint16(port))
	req = append(req, portBuf...)

	if err := writeAll(conn, req); err != nil {
		return err
	}

	reply := make([]byte, 4)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return err
	}
	if reply[0] != socks5Version {
		return fmt.Errorf("socks5 reply version invalid: %d", reply[0])
	}
	if reply[1] != 0x00 {
		return fmt.Errorf("socks5 connect failed: %d", reply[1])
	}
	addrLen, err := socks5ReplyAddrLen(conn, reply[3])
	if err != nil {
		return err
	}
	if addrLen > 0 {
		dump := make([]byte, addrLen)
		if _, err := io.ReadFull(conn, dump); err != nil {
			return err
		}
	}
	portReply := make([]byte, 2)
	if _, err := io.ReadFull(conn, portReply); err != nil {
		return err
	}
	return nil
}

func encodeSocks5Addr(host string) (byte, []byte, error) {
	ip := net.ParseIP(host)
	if ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			return socks5ATypV4, ip4, nil
		}
		ip6 := ip.To16()
		if ip6 == nil {
			return 0, nil, fmt.Errorf("invalid ip: %s", host)
		}
		return socks5ATypV6, ip6, nil
	}
	if len(host) > 255 {
		return 0, nil, fmt.Errorf("domain too long: %s", host)
	}
	out := make([]byte, 1+len(host))
	out[0] = byte(len(host))
	copy(out[1:], host)
	return socks5ATypDNS, out, nil
}

func socks5ReplyAddrLen(conn net.Conn, atyp byte) (int, error) {
	switch atyp {
	case socks5ATypV4:
		return net.IPv4len, nil
	case socks5ATypV6:
		return net.IPv6len, nil
	case socks5ATypDNS:
		length := make([]byte, 1)
		if _, err := io.ReadFull(conn, length); err != nil {
			return 0, err
		}
		return int(length[0]), nil
	default:
		return 0, fmt.Errorf("socks5 invalid atyp: %d", atyp)
	}
}

func writeAll(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func combineErrors(primary, secondary error) error {
	if secondary == nil {
		return primary
	}
	if primary == nil {
		return secondary
	}
	return fmt.Errorf("%v; close error: %w", primary, secondary)
}
