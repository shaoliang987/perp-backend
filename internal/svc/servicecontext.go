package svc

import (
	"metanode/internal/chain"
	"metanode/internal/config"
	"metanode/internal/engine"
	"metanode/internal/model"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

// ServiceContext 服务上下文
type ServiceContext struct {
	Config config.Config

	// 数据库连接
	DB sqlx.SqlConn

	// Redis 连接
	Redis *redis.Redis

	// 数据模型
	OrderModel       model.OrderModel
	TradeModel       model.TradeModel
	PositionModel    model.PositionModel
	DepositModel     model.DepositModel
	WithdrawModel    model.WithdrawModel
	FundingRateModel model.FundingRateModel
	LiquidationModel model.LiquidationModel
	MarketModel      model.MarketModel
	KlineModel       model.KlineModel

	// Chain 为 nil 时：无 RPC，链上查询接口返回空或零；撮合仍可记库但不会发交易。
	Chain *chain.Client
	// MatchEngine 在 main 中创建并赋值，供 CreateOrder 入簿。
	MatchEngine *engine.MatchEngine
}

// NewServiceContext 创建服务上下文
func NewServiceContext(c config.Config) *ServiceContext {
	// 初始化 MySQL 连接
	db := sqlx.NewMysql(c.MySQL.DataSource)

	// 初始化 Redis 连接
	rds := redis.MustNewRedis(redis.RedisConf{
		Host: c.Redis.Host,
		Type: c.Redis.Type,
		Pass: c.Redis.Pass,
	})

	return &ServiceContext{
		Config: c,
		DB:     db,
		Redis:  rds,

		// 初始化数据模型
		OrderModel:       model.NewOrderModel(db),
		TradeModel:       model.NewTradeModel(db),
		PositionModel:    model.NewPositionModel(db),
		DepositModel:     model.NewDepositModel(db),
		WithdrawModel:    model.NewWithdrawModel(db),
		FundingRateModel: model.NewFundingRateModel(db),
		LiquidationModel: model.NewLiquidationModel(db),
		MarketModel:      model.NewMarketModel(db),
		KlineModel:       model.NewKlineModel(db),
	}
}
