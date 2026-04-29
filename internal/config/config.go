package config

import (
	"github.com/zeromicro/go-zero/rest"
)

// Config 应用配置
type Config struct {
	rest.RestConf

	// MySQL 配置
	MySQL MySQLConfig

	// Redis 配置
	Redis RedisConfig

	// 以太坊配置
	Ethereum EthereumConfig

	// 市场配置
	Markets []MarketConfig

	// 撮合引擎配置
	MatchEngine MatchEngineConfig

	// 资金费率配置
	FundingRate FundingRateConfig

	// 清算机器人配置
	Liquidator LiquidatorConfig

	// Chainlink 预言机配置
	Chainlink ChainlinkConfig
}

// MySQLConfig MySQL 配置
type MySQLConfig struct {
	DataSource string
}

// RedisConfig Redis 配置
type RedisConfig struct {
	Host string
	Type string
	Pass string
}

// EthereumConfig 以太坊配置
type EthereumConfig struct {
	RpcUrl             string
	ChainId            int64
	DealerAddress      string
	PrivateKey         string
	GasPriceMultiplier float64
	MaxGasLimit        int64
}

// MarketConfig 市场配置
type MarketConfig struct {
	Address     string
	Name        string
	PriceSource string
}

// MatchEngineConfig 撮合引擎配置
type MatchEngineConfig struct {
	MatchInterval    int
	BatchSize        int
	MaxPendingOrders int
}

// FundingRateConfig 资金费率配置
type FundingRateConfig struct {
	SettleInterval int
	MaxRate        string
}

// LiquidatorConfig 清算机器人配置
type LiquidatorConfig struct {
	CheckInterval int
	Enabled       bool
}

// ChainlinkConfig Chainlink 预言机配置
type ChainlinkConfig struct {
	BTC_USD string
	ETH_USD string
}
