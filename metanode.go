package main

import (
	"flag"
	"fmt"

	"metanode/internal/chain"
	"metanode/internal/config"
	"metanode/internal/engine"
	"metanode/internal/handler"
	"metanode/internal/handler/market"
	"metanode/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String("f", "etc/metanode.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	ctx := svc.NewServiceContext(c)

	// 链客户端：失败时 Chain 为 nil，仅 HTTP 可用，撮合/资金费不落链。
	ch, err := chain.NewClient(c.Ethereum, c.Markets, c.Chainlink)
	if err != nil {
		fmt.Println("chain client:", err, "(链上查询/结算将不可用，请检查 RpcUrl、DealerAddress)")
	} else {
		ctx.Chain = ch
		defer ch.Close()
	}

	// 撮合引擎与 ServiceContext 互相引用，便于下单 API 入簿。
	matchEngine := engine.NewMatchEngine(c.MatchEngine, ctx.DB, ctx.Chain)
	ctx.MatchEngine = matchEngine

	handler.RegisterHandlers(server, ctx)
	market.RegisterTickerWebSocket(server, ctx)

	matchEngine.Start()
	defer matchEngine.Stop()

	// 启动清算机器人
	liquidator := engine.NewLiquidator(c.Liquidator, c.Ethereum, ctx.DB)
	liquidator.Start()
	defer liquidator.Stop()

	// 启动资金费率服务
	fundingKeeper := engine.NewFundingRateKeeper(c.FundingRate, c.Markets, ctx.DB, ctx.Chain)
	fundingKeeper.Start()
	defer fundingKeeper.Stop()

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
