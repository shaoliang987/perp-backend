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
	ticker := time.NewTicker(time.Duration(e.config.MatchInterval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.match()
		}
	}
}

// match 执行撮合
func (e *MatchEngine) match() {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, orderBook := range e.orderBooks {
		e.matchOrderBook(orderBook)
	}
}

// matchOrderBook 撮合单个订单簿
func (e *MatchEngine) matchOrderBook(book *OrderBook) {
	book.mu.Lock()
	defer book.mu.Unlock()

	for len(book.BuyOrders) > 0 && len(book.SellOrders) > 0 {
		buyOrder := book.BuyOrders[0]
		sellOrder := book.SellOrders[0]

		buyPrice := calculatePrice(buyOrder)
		sellPrice := calculatePrice(sellOrder)

		if buyPrice.Cmp(sellPrice) < 0 {
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
			break
		}

		result := &MatchResult{
			TakerOrder:  buyOrder,
			MakerOrder:  sellOrder,
			MatchAmount: matchAmount.String(),
			MatchPrice:  sellPrice.String(),
		}

		e.tradeMu.Lock()
		e.pendingTrades = append(e.pendingTrades, result)
		e.tradeMu.Unlock()

		e.remMu.Lock()
		e.remain[buyOrder.OrderId].Sub(e.remain[buyOrder.OrderId], matchAmount)
		e.remain[sellOrder.OrderId].Sub(e.remain[sellOrder.OrderId], matchAmount)
		if e.remain[buyOrder.OrderId].Sign() == 0 {
			book.BuyOrders = book.BuyOrders[1:]
			delete(e.remain, buyOrder.OrderId)
		}
		if e.remain[sellOrder.OrderId].Sign() == 0 {
			book.SellOrders = book.SellOrders[1:]
			delete(e.remain, sellOrder.OrderId)
		}
		e.remMu.Unlock()
	}
}

// submitLoop 提交交易循环
func (e *MatchEngine) submitLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
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
	for _, trade := range trades {
		matchAmt, ok := new(big.Int).SetString(trade.MatchAmount, 10)
		if !ok {
			logx.Errorf("bad match amount %s", trade.MatchAmount)
			continue
		}

		var txHash string
		var blockNum int64
		if e.chain != nil {
			td, err := chain.BuildMatchTradeData(trade.TakerOrder, trade.MakerOrder, matchAmt)
			if err != nil {
				logx.Errorf("build trade data: %v", err)
			} else {
				perp := common.HexToAddress(trade.TakerOrder.Perp)
				tx, err := e.chain.SubmitPerpTrade(ctx, perp, td)
				if err != nil {
					logx.Errorf("SubmitPerpTrade failed: %v", err)
				} else {
					txHash = tx.Hash().Hex()
					if rec, err := bind.WaitMined(ctx, e.chain.RPC(), tx); err == nil && rec != nil {
						blockNum = int64(rec.BlockNumber.Uint64())
						if rec.Status != 1 {
							logx.Errorf("trade tx reverted: %s", txHash)
						}
					} else if err != nil {
						logx.Errorf("WaitMined: %v", err)
					}
				}
			}
		} else {
			logx.Info("Chain client nil, skip on-chain settle (dev mode)")
		}

		e.applyFillStatus(ctx, trade.TakerOrder.OrderId, trade.TakerOrder.PaperAmount, matchAmt.String())
		e.applyFillStatus(ctx, trade.MakerOrder.OrderId, trade.MakerOrder.PaperAmount, matchAmt.String())

		if _, err := e.tradeModel.Insert(ctx, &model.Trade{
			TradeId:      generateTradeId(),
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
			logx.Errorf("insert trade: %v", err)
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
