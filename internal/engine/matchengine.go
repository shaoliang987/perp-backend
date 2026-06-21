package engine

// 撮合引擎：内存订单簿 + 周期性调用 chain.BuildMatchTradeData / Perpetual.trade。
// 链上角色：私钥地址须为 Dealer.validOrderSender（见 SubmitPerpTrade）。

import (
	"context"
	"math/big"
	"sort"
	"sync"
	"time"

	"metanode/internal/chain"
	"metanode/internal/config"
	"metanode/internal/model"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

// MatchEngine 内存撮合 +（可选）链上结算。约定：买单为 taker、卖单为 maker，与 Trading._matchOrders 一致。
type MatchEngine struct {
	config     config.MatchEngineConfig
	orderModel model.OrderModel
	tradeModel model.TradeModel
	chain      *chain.Client

	// 订单簿：perp -> side -> orders
	orderBooks map[string]*OrderBook
	mu         sync.RWMutex

	// orderId -> 剩余可撮合 paper 绝对值（与链上累计成交量配合；订单对象仍保留签名时的完整 paper/credit）
	remain map[string]*big.Int
	remMu  sync.Mutex

	// 待提交的交易
	pendingTrades []*MatchResult
	tradeMu       sync.Mutex

	stopCh chan struct{}
}

// OrderBook 订单簿
type OrderBook struct {
	Perp       string
	BuyOrders  []*model.Order // 买单（做多），按价格从高到低排序
	SellOrders []*model.Order // 卖单（做空），按价格从低到高排序
	mu         sync.RWMutex
}

// MatchResult 撮合结果
type MatchResult struct {
	TakerOrder  *model.Order
	MakerOrder  *model.Order
	MatchAmount string // 成交数量（paper 绝对值）
	MatchPrice  string // 成交价格
}

// NewMatchEngine 创建撮合引擎
func NewMatchEngine(cfg config.MatchEngineConfig, db sqlx.SqlConn, ch *chain.Client) *MatchEngine {
	return &MatchEngine{
		config:     cfg,
		orderModel: model.NewOrderModel(db),
		tradeModel: model.NewTradeModel(db),
		chain:      ch,
		orderBooks: make(map[string]*OrderBook),
		remain:     make(map[string]*big.Int),
		stopCh:     make(chan struct{}),
	}
}

// Start 启动撮合引擎
func (e *MatchEngine) Start() {
	logx.Info("Match engine starting...")

	go e.matchLoop()
	go e.submitLoop()
}

// Stop 停止撮合引擎
func (e *MatchEngine) Stop() {
	close(e.stopCh)
}

func absPaperString(paper string) *big.Int {
	z := new(big.Int)
	z.SetString(paper, 10)
	z.Abs(z)
	return z
}

// AddOrder 添加订单到订单簿
func (e *MatchEngine) AddOrder(order *model.Order) {
	e.remMu.Lock()
	e.remain[order.OrderId] = absPaperString(order.PaperAmount)
	e.remMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()

	// 获取或创建订单簿
	orderBook, ok := e.orderBooks[order.Perp]
	if !ok {
		orderBook = &OrderBook{Perp: order.Perp}
		e.orderBooks[order.Perp] = orderBook
	}

	orderBook.mu.Lock()
	defer orderBook.mu.Unlock()

	// 根据订单方向添加到对应队列
	paperAmount := new(big.Int)
	paperAmount.SetString(order.PaperAmount, 10)

	if paperAmount.Sign() > 0 {
		orderBook.BuyOrders = append(orderBook.BuyOrders, order)
		sort.Slice(orderBook.BuyOrders, func(i, j int) bool {
			priceI := calculatePrice(orderBook.BuyOrders[i])
			priceJ := calculatePrice(orderBook.BuyOrders[j])
			return priceI.Cmp(priceJ) > 0
		})
	} else {
		orderBook.SellOrders = append(orderBook.SellOrders, order)
		sort.Slice(orderBook.SellOrders, func(i, j int) bool {
			priceI := calculatePrice(orderBook.SellOrders[i])
			priceJ := calculatePrice(orderBook.SellOrders[j])
			return priceI.Cmp(priceJ) < 0
		})
	}
}

// matchLoop 撮合循环
func (e *MatchEngine) matchLoop() {
	logx.Infof("[MatchLoop] 撮合协程已启动，执行周期: %d ms", e.config.MatchInterval)
	ticker := time.NewTicker(time.Duration(e.config.MatchInterval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			logx.Info("[MatchLoop] 收到停止信号，撮合协程退出")
			return
		case <-ticker.C:
			// 开始新一轮撮合
			e.match()
		}
	}
}

// match 执行撮合
func (e *MatchEngine) match() {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 如果整个引擎连一个订单簿都还没初始化（空系统状态）
	if len(e.orderBooks) == 0 {
		logx.Infof("[MatchLoop 心跳] 当前无任何活跃交易对(OrderBook为空)，等待订单注入...")
		return
	}

	for _, orderBook := range e.orderBooks {
		e.matchOrderBook(orderBook)
	}
}

// matchOrderBook 撮合单个订单簿
func (e *MatchEngine) matchOrderBook(book *OrderBook) {
	book.mu.Lock()
	defer book.mu.Unlock()

	// 仅在有挂单时输出当前队列状态，避免空轮询刷屏
	if len(book.BuyOrders) > 0 || len(book.SellOrders) > 0 {
		logx.Debugf("[MatchEngine] [%s] 开始撮合. 队列状态 -> 買单(Taker)数量: %d, 賣单(Maker)数量: %d",
			book.Perp, len(book.BuyOrders), len(book.SellOrders))
	}

	for len(book.BuyOrders) > 0 && len(book.SellOrders) > 0 {
		buyOrder := book.BuyOrders[0]
		sellOrder := book.SellOrders[0]

		buyPrice := calculatePrice(buyOrder)
		sellPrice := calculatePrice(sellOrder)

		if buyPrice.Cmp(sellPrice) < 0 {
			// 价格无法交叉，退出当前币对的撮合
			logx.Debugf("[MatchEngine] [%s] 价格未交叉. 最高買价: %s, 最低賣价: %s. 结束本轮撮合.",
				book.Perp, buyPrice.String(), sellPrice.String())
			break
		}

		e.remMu.Lock()
		buyRem := new(big.Int).Set(e.remain[buyOrder.OrderId])
		sellRem := new(big.Int).Set(e.remain[sellOrder.OrderId])
		e.remMu.Unlock()

		matchAmount := new(big.Int)
		if buyRem.Cmp(sellRem) < 0 {
			matchAmount.Set(buyRem)
		} else {
			matchAmount.Set(sellRem)
		}

		if matchAmount.Sign() == 0 {
			logx.Errorf("[MatchEngine] [%s] 异常: 队列头部订单剩余量为0. 買单ID: %s, 賣单ID: %s.",
				book.Perp, buyOrder.OrderId, sellOrder.OrderId)
			break
		}

		result := &MatchResult{
			TakerOrder:  buyOrder,
			MakerOrder:  sellOrder,
			MatchAmount: matchAmount.String(),
			MatchPrice:  sellPrice.String(),
		}

		// 核心撮合成功日志 (INFO 级别)
		logx.Infof("[✔ 撮合成功] [%s] 買单(Taker): %s | 賣单(Maker): %s | 撮合价: %s | 撮合数量: %s",
			book.Perp, buyOrder.OrderId, sellOrder.OrderId, result.MatchPrice, result.MatchAmount)

		e.tradeMu.Lock()
		e.pendingTrades = append(e.pendingTrades, result)
		e.tradeMu.Unlock()

		e.remMu.Lock()
		e.remain[buyOrder.OrderId].Sub(e.remain[buyOrder.OrderId], matchAmount)
		e.remain[sellOrder.OrderId].Sub(e.remain[sellOrder.OrderId], matchAmount)

		if e.remain[buyOrder.OrderId].Sign() == 0 {
			logx.Infof("[订单吃满] 買单完全成交并全额移出队列. ID: %s", buyOrder.OrderId)
			book.BuyOrders = book.BuyOrders[1:]
			delete(e.remain, buyOrder.OrderId)
		} else {
			logx.Debugf("[部分成交] 買单仍有剩余. ID: %s, 剩余量: %s", buyOrder.OrderId, e.remain[buyOrder.OrderId].String())
		}

		if e.remain[sellOrder.OrderId].Sign() == 0 {
			logx.Infof("[订单吃满] 賣单完全成交并全额移出队列. ID: %s", sellOrder.OrderId)
			book.SellOrders = book.SellOrders[1:]
			delete(e.remain, sellOrder.OrderId)
		} else {
			logx.Debugf("[部分成交] 賣单仍有剩余. ID: %s, 剩余量: %s", sellOrder.OrderId, e.remain[sellOrder.OrderId].String())
		}
		e.remMu.Unlock()
	}
}

// submitLoop 提交交易循环
func (e *MatchEngine) submitLoop() {
	logx.Info("[SubmitLoop] 链上结算提交协程已启动，执行周期: 1 秒")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			logx.Info("[SubmitLoop] 收到停止信号，提交协程退出")
			return
		case <-ticker.C:
			e.submitTrades()
		}
	}
}

// submitTrades 提交交易到链上
func (e *MatchEngine) submitTrades() {
	e.tradeMu.Lock()
	if len(e.pendingTrades) == 0 {
		e.tradeMu.Unlock()
		return
	}

	trades := e.pendingTrades
	e.pendingTrades = nil
	e.tradeMu.Unlock()

	batch := make([]*MatchResult, 0, e.config.BatchSize)
	for _, trade := range trades {
		batch = append(batch, trade)
		if len(batch) >= e.config.BatchSize {
			e.submitBatch(batch)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		e.submitBatch(batch)
	}
}

// submitBatch 对每笔撮合：编码 tradeData → Perpetual.trade → 等待收据；然后更新 DB。
// 注意：链上失败时仍会更新库表，生产环境可改为仅成功落库或回滚 remain（当前为简化实现）。
func (e *MatchEngine) submitBatch(trades []*MatchResult) {
	ctx := context.Background()
	logx.Infof("[SubmitBatch] 开始处理批量链上结算，本次批大小(BatchSize): %d", len(trades))

	for i, trade := range trades {
		matchAmt, ok := new(big.Int).SetString(trade.MatchAmount, 10)
		if !ok {
			logx.Errorf("[SubmitBatch] 错误: 异常的成交数量 %s", trade.MatchAmount)
			continue
		}

		var txHash string
		var blockNum int64

		if e.chain != nil {
			logx.Infof("[SubmitBatch] [%d/%d] 正在构建链上数据... Taker: %s, Maker: %s", i+1, len(trades), trade.TakerOrder.OrderId, trade.MakerOrder.OrderId)

			td, err := chain.BuildMatchTradeData(trade.TakerOrder, trade.MakerOrder, matchAmt)
			if err != nil {
				logx.Errorf("[SubmitBatch] 构建 TradeData 失败: %v", err)
				continue
			}

			perp := common.HexToAddress(trade.TakerOrder.Perp)

			// 发送交易
			logx.Infof("[SubmitBatch] 正在发送智能合约交易 SubmitPerpTrade -> 永续合约: %s", trade.TakerOrder.Perp)
			tx, err := e.chain.SubmitPerpTrade(ctx, perp, td)
			if err != nil {
				logx.Errorf("[SubmitBatch] 链上 SubmitPerpTrade 交易发送失败: %v", err)
			} else {
				txHash = tx.Hash().Hex()
				logx.Infof("[⛓ 交易已发出] 哈希: %s, 等待区块打包确认 (WaitMined)...", txHash)

				// 同步等待收据
				if rec, err := bind.WaitMined(ctx, e.chain.RPC(), tx); err == nil && rec != nil {
					blockNum = int64(rec.BlockNumber.Uint64())
					if rec.Status == 1 {
						logx.Infof("[★ 链上确认成功] 交易已被打包. 区块号: %d, TxHash: %s", blockNum, txHash)
					} else {
						logx.Errorf("[💥 链上执行 Revert] 交易打包但执行失败(Status=0). TxHash: %s", txHash)
					}
				} else if err != nil {
					logx.Errorf("[SubmitBatch] 等待区块打包确认(WaitMined)时出错: %v", err)
				}
			}
		} else {
			logx.Slowf("[SubmitBatch] (开发模式) Chain 客户端为 nil，跳过上链。仅模拟落库。")
		}

		// 更新订单状态及入库
		logx.Infof("[DB] 正在更新订单状态与持久化 Trade 数据. TakerOrder: %s, MakerOrder: %s", trade.TakerOrder.OrderId, trade.MakerOrder.OrderId)
		e.applyFillStatus(ctx, trade.TakerOrder.OrderId, trade.TakerOrder.PaperAmount, matchAmt.String())
		e.applyFillStatus(ctx, trade.MakerOrder.OrderId, trade.MakerOrder.PaperAmount, matchAmt.String())

		tradeID := generateTradeId()
		if _, err := e.tradeModel.Insert(ctx, &model.Trade{
			TradeId:      tradeID,
			Perp:         trade.TakerOrder.Perp,
			TakerOrderId: trade.TakerOrder.OrderId,
			MakerOrderId: trade.MakerOrder.OrderId,
			Taker:        trade.TakerOrder.Signer,
			Maker:        trade.MakerOrder.Signer,
			PaperAmount:  trade.MatchAmount,
			Price:        trade.MatchPrice,
			TxHash:       txHash,
			BlockNumber:  blockNum,
			CreateTime:   time.Now(),
		}); err != nil {
			logx.Errorf("[DB] 插入历史成交表失败: %v", err)
		} else {
			logx.Infof("[DB ✔] 成交流水本地落库成功. 流水号(TradeId): %s", tradeID)
		}
	}
}

func (e *MatchEngine) applyFillStatus(ctx context.Context, orderID, signedPaper, deltaStr string) {
	// 按订单原始签名数量累计 filled；达到全额则 Status=Filled，否则 PartialFill。
	o, err := e.orderModel.FindOne(ctx, orderID)
	if err != nil || o == nil {
		return
	}
	full := absPaperString(signedPaper)
	delta, _ := new(big.Int).SetString(deltaStr, 10)
	prev, _ := new(big.Int).SetString(o.FilledAmount, 10)
	total := new(big.Int).Add(prev, delta)
	status := model.OrderStatusPartialFill
	if total.Cmp(full) >= 0 {
		status = model.OrderStatusFilled
		total.Set(full)
	}
	_ = e.orderModel.UpdateStatus(ctx, orderID, status, total.String())
}

// calculatePrice 计算订单价格 |credit|/|paper|（1e18 精度）
func calculatePrice(order *model.Order) *big.Int {
	paper := new(big.Int)
	paper.SetString(order.PaperAmount, 10)
	paper.Abs(paper)

	credit := new(big.Int)
	credit.SetString(order.CreditAmount, 10)
	credit.Abs(credit)

	if paper.Sign() == 0 {
		return big.NewInt(0)
	}

	precision := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	price := new(big.Int).Mul(credit, precision)
	price.Div(price, paper)
	return price
}

func generateTradeId() string {
	return time.Now().Format("20060102150405") + randomString(8)
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
	}
	return string(b)
}
