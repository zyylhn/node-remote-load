package remote_load

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kataras/golog"
	"github.com/zyylhn/node-remote-load/moduleArgs"
	"github.com/zyylhn/node-tree/admin"
	"github.com/zyylhn/node-tree/admin/initial"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
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
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	//根据接口获取参数
	args, err := module.GetArgs()
	if err != nil {
		cancel()
		return nil, err
	}
	//将参数按需二次处理放到参数服务器或者直接传输,最后得到模块真实接收的参数列表
	var newArgs moduleArgs.ModeRequest
	for _, arg := range args {
		var request moduleArgs.RequestInfo
		if arg.ArgType == moduleArgs.OnlyBackward {
			request.Type = moduleArgs.OnlyBackward
			//todo 仅生成一个反向端口转发
		} else if arg.ArgType == moduleArgs.PushResultAddr {
			//需要指定推送结果的地址，模块接收到此类型的参数将会向指定地址推送消息
			request.Type = moduleArgs.PushResultAddr
			//todo 开启服务器并进行反向端口转发，将结果送到channel中
		} else if len(arg.Arg) > 512 || arg.ArgType == moduleArgs.ArgsAddr {
			//使用参数服务器
			if arg.ArgType != 0 {
				request.Type = arg.ArgType
			} else {
				request.Type = moduleArgs.ArgsAddr
			}
			//todo 生成一个参数服务器
			//request.Addr, err = GenerateArgsServer(arg.Arg, mgr, route, uuid, &newCtx, log)
			//if err != nil {
			//	return nil, err
			//}
		} else {
			//使用正常方式传输
			if arg.ArgType != 0 {
				request.Type = arg.ArgType
			} else {
				request.Type = moduleArgs.Args
			}
			request.Args = arg.Arg
		}
		newArgs = append(newArgs, request)
	}
	//根据基本信息接口获取到模块的内容,使用重试机制来避免模块升级短暂导致模块不存在本地文件的情况
	retryNum := 5
	var moduleFile *os.File
	var readErr error
	success := false
	for i := 0; i < retryNum; i++ {
		moduleFile, readErr = os.Open(module.GetInfo().ModulePath)
		if readErr != nil {
			time.Sleep(time.Second * 2)
			continue
		} else {
			success = true
			break
		}
	}
	if !success {
		return nil, fmt.Errorf("读取模块%v:%v出现异常:%v", module.GetInfo().Name, module.GetInfo().ModulePath, readErr)
	}
	var argsJson []byte
	//因为部分模块不需要参数，所以newArgs可能为空
	if newArgs != nil {
		argsJson, err = json.Marshal(&newArgs)
		if err != nil {
			return nil, err
		}
	}
	remoteModule, err := r.CreateRemoteLoad(newCtx, moduleFile, string(argsJson), module.GetInfo().Name)
	if err != nil {
		return nil, err
	}
	re, err := remoteModule.LoadExec(node, "")
	if err != nil {
		return nil, err
	}
	return r.regexpFitResult(re)
}

// 在指定节点上开启一个连接lAddr的反向端口转发，并返回在agent上开启的端口号
func (r *RemoteLoad) startBackWard(ctx context.Context, uuid string, lAddr string) (string, error) {
	var n int
	var success bool
	//在目标上监听端口做转发可能出现端口被占用的情况，这栗会采用从新获取端口多次尝试的方式
	rand.Seed(time.Now().Unix())
	for i := 0; i < 99; i++ {
		n = rand.Intn(55534-99) + 10000 + i //10000-65535
		err := r.CreateBackward(lAddr, fmt.Sprintf("%v", n), uuid)
		if err != nil {
			//already in use 是unix中的报错，only one是windows中的报错
			if strings.Contains(err.Error(), "address already in use") || strings.Contains(err.Error(), "Only one usage of each socket address") {
				time.Sleep(time.Millisecond * 100)
				r.log.Warn(fmt.Sprintf("[server based on Backward(listen on admin:%v,agent:%v)]listen port %v on %v worn:already in use,start retry", lAddr, n, n, uuid))
				continue
			} else {
				return "", err
			}
		} else {
			success = true
			break
		}
	}
	if !success {
		return "", fmt.Errorf("99 attempts to listen to random ports on agent%v failed. The reasons for the failure are described in the log", uuid)
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = r.StopBackward(uuid, strconv.Itoa(n))
		}
	}()
	return fmt.Sprintf("%v", n), nil
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
