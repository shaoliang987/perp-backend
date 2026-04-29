package trade

import (
	"context"

	"metanode/internal/svc"
	"metanode/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListTradesLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// 查询成交记录
func NewListTradesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListTradesLogic {
	return &ListTradesLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListTradesLogic) ListTrades(req *types.ListTradesReq) (resp *types.ListTradesResp, err error) {
	// todo: add your logic here and delete this line

	return
}
