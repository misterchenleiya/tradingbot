package transport

import commontransport "github.com/misterchenleiya/tradingbot/common/transport"

type ProxyDialer = commontransport.ProxyDialer

func NewProxyDialer(raw string) (*ProxyDialer, error) {
	return commontransport.NewProxyDialer(raw)
}

func CanonicalProxyAddress(raw string) (string, error) {
	return commontransport.CanonicalProxyAddress(raw)
}
