package market

const (
	EndpointOHLCVLatest   = "ohlcv_latest"
	EndpointOHLCVRange    = "ohlcv_range"
	EndpointMarkets       = "markets"
	EndpointDailyVolumes  = "daily_volumes"
	EndpointWSSubscribe   = "ws_subscribe"
	EndpointWSUnsubscribe = "ws_unsubscribe"
	EndpointWSStream      = "ws_stream"
)

type RequestMeta struct {
	Exchange string
	Endpoint string
	Weight   int
	MaxBars  int
	Priority int
	Realtime bool
}
