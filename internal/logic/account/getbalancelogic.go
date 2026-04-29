package account

// GetBalance：账户余额来自 Dealer.getCreditOf（保证金在 credit 中，非单独 ERC20 余额）。

import (
	"context"

	"metanode/internal/svc"
	"metanode/internal/types"

	"github.com/ethereum/go-ethereum/common"
	"github.com/zeromicro/go-zero/core/logx"
)

type GetBalanceLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetBalanceLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetBalanceLogic {
	return &GetBalanceLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetBalanceLogic) GetBalance(req *types.GetBalanceReq) (resp *types.GetBalanceResp, err error) {
	if l.svcCtx.Chain == nil {
		return &types.GetBalanceResp{Code: 0, Message: "ok", Balance: types.AccountBalance{Trader: req.Trader}}, nil
	}
	pc, sc, pp, ps, ts, err := l.svcCtx.Chain.GetCreditOf(l.ctx, common.HexToAddress(req.Trader))
	if err != nil {
		return &types.GetBalanceResp{Code: 500, Message: err.Error()}, nil
	}
	return &types.GetBalanceResp{
		Code:    0,
		Message: "ok",
		Balance: types.AccountBalance{
			Trader:                   req.Trader,
			PrimaryCredit:            pc.String(),
			SecondaryCredit:          sc.String(),
			PendingPrimaryWithdraw:   pp.String(),
			PendingSecondaryWithdraw: ps.String(),
			ExecutionTimestamp:       ts.Int64(),
		},
	}, nil
}
