package remote_load

import (
	"context"
	"fmt"
	"github.com/kataras/golog"
	"github.com/zyylhn/node-remote-load/moduleArgs"
	"github.com/zyylhn/node-tree/admin"
	"github.com/zyylhn/node-tree/admin/initial"
	"regexp"
)

// GetModuleArgs 获取执行参数接口
type GetModuleArgs interface {
	GetArgs() ([]moduleArgs.ArgWithType, error) //每个模块类型通过此方式获取参数
}

// GetModuleInfo 获取模块信息的接口
type GetModuleInfo interface {
	GetInfo() *ModuleInfo //返回模块信息
}

// ModuleLoad 加载模块的接口
type ModuleLoad interface {
	GetModuleArgs
	GetModuleInfo
}

type RemoteLoad struct {
	*admin.NodeManager
	log *golog.Logger
}

func NewRemoteLoad(op *initial.Options, log *golog.Logger) *RemoteLoad {
	re := &RemoteLoad{
		NodeManager: admin.NewAdmin(op, log.Clone()),
	}
	re.log = log.SetPrefix("<remote-load> ")

	return re
}

func (r *RemoteLoad) StartNodeManager(ctx context.Context, pushChan chan interface{}) error {
	return r.Start(ctx, pushChan)
}

// RemoteLoadOnNode 在指定节点上加载程序
func (r *RemoteLoad) RemoteLoadOnNode(ctx context.Context, module ModuleLoad, node string, resultChan chan []byte) ([]byte, error) {
	return nil, nil
}

func (r *RemoteLoad) remoteLoad(ctx context.Context, module ModuleLoad, node string, resultChan chan []byte) ([]byte, error) {
	return nil, nil
}

// 在模块执行返回的结果中匹配我们想要的结果
func (r *RemoteLoad) regexpFitResult(re []byte) ([]byte, error) {
	pattern := `(({\"module_name\":|{\"data\":|{\"error\":|{\"time\":).*?(\"time\":\".*?\"}|\"error\":\".*?\"}|\"data\":\".*?\"}|\"module_name\":\".*?\"}))`
	reg := regexp.MustCompile(pattern)
	matches := reg.FindAllString(string(re), -1)
	if len(matches) > 0 {
		return []byte(matches[0]), nil
	}
	return nil, fmt.Errorf("did not match the standardized result of the module output")
}
