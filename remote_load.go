package remote_load

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/kataras/golog"
	"github.com/zyylhn/getlocaladdr"
	"github.com/zyylhn/node-remote-load/moduleArgs"
	"github.com/zyylhn/node-tree/admin"
	"github.com/zyylhn/node-tree/admin/initial"
	"io"
	"math/rand"
	"net"
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
}

func NewRemoteLoad(op *initial.Options, log *golog.Logger) *RemoteLoad {
	re := &RemoteLoad{
		NodeManager: admin.NewAdmin(op, log.Clone()),
	}

	return re
}

func (r *RemoteLoad) StartNodeManager(ctx context.Context, pushChan chan interface{}) error {
	return r.Start(ctx, pushChan)
}

// RemoteLoadOnNode 在指定节点上加载程序
func (r *RemoteLoad) RemoteLoadOnNode(ctx context.Context, module ModuleLoad, node string, resultChan chan []byte, log *golog.Logger) ([]byte, error) {
	return r.remoteLoad(ctx, "", module, node, resultChan, log)
}

func (r *RemoteLoad) RemoteLoadOnNodeWithID(ctx context.Context, id string, module ModuleLoad, node string, resultChan chan []byte, log *golog.Logger) ([]byte, error) {
	return r.remoteLoad(ctx, id, module, node, resultChan, log)
}

func (r *RemoteLoad) remoteLoad(ctx context.Context, taskID string, module ModuleLoad, node string, resultChan chan []byte, log *golog.Logger) ([]byte, error) {
	log = log.Clone()
	log.SetPrefix(fmt.Sprintf("%v <%v:%v>", log.Prefix, "remoteLoad", module.GetInfo().Name))
	if resultChan != nil {
		defer close(resultChan)
	}
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
			rPort, err := r.startBackWard(ctx, node, string(arg.Arg), log)
			if err != nil {
				return nil, err
			}
			request.Addr = fmt.Sprintf("127.0.0.1:%v", rPort)
		} else if arg.ArgType == moduleArgs.PushResultAddr {
			//需要指定推送结果的地址，模块接收到此类型的参数将会向指定地址推送消息
			request.Type = moduleArgs.PushResultAddr
			rPort, err := r.createReceiveHandle(ctx, resultChan, node, log)
			if err != nil {
				return nil, err
			}
			request.Addr = fmt.Sprintf("127.0.0.1:%v", rPort)
		} else if len(arg.Arg) > 512 || arg.ArgType == moduleArgs.ArgsAddr {
			//使用参数服务器
			if arg.ArgType != 0 {
				request.Type = arg.ArgType
			} else {
				request.Type = moduleArgs.ArgsAddr
			}
			rPort, err := r.createArgsServer(ctx, arg.Arg, node, log)
			if err != nil {
				return nil, err
			}
			request.Addr = fmt.Sprintf("127.0.0.1:%v", rPort)
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
	remoteModule, err := r.Mgr.RemoteLoadManager.NewRemoteLoadWithTaskID(newCtx, taskID, moduleFile, string(argsJson), module.GetInfo().Name)
	if err != nil {
		return nil, err
	}
	re, err := remoteModule.LoadExec(node, "")
	if err != nil {
		return nil, err
	}
	return r.regexpFitResult(re)
}

func (r *RemoteLoad) createArgsServer(ctx context.Context, arg []byte, uuid string, log *golog.Logger) (string, error) {
	listener, err := r.startListener(ctx, log)
	if err != nil {
		return "", err
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") { //listener关闭
					break
				} else {
					log.Errorf("[server based on Backward(listen on admin:%v,agent:%v)]LocalServer accept error:"+err.Error(), listener.Addr().String())
					break
				}
			}
			r.argServerHandle(ctx, conn, arg, log)
		}
	}()
	return r.startBackWard(ctx, uuid, listener.Addr().String(), log)
}

func (r *RemoteLoad) createReceiveHandle(ctx context.Context, receiveChan chan []byte, uuid string, log *golog.Logger) (string, error) {
	listener, err := r.startListener(ctx, log)
	if err != nil {
		return "", err
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") { //listener关闭
					break
				} else {
					log.Errorf("[server based on Backward(listen on admin:%v,agent:%v)]LocalServer accept error:"+err.Error(), listener.Addr().String())
					break
				}
			}
			r.receiveHandle(ctx, conn, receiveChan, log)
		}
	}()
	return r.startBackWard(ctx, uuid, listener.Addr().String(), log)
}

func (r *RemoteLoad) receiveHandle(ctx context.Context, conn net.Conn, receiveChan chan []byte, log *golog.Logger) {
	defer func() {
		_ = conn.Close()
		//还是有可能触发向关闭的channel发送数据
		if err := recover(); err != nil {
			log.Errorf("[server based on Backward(listen on admin:%v)]receiveHandle panic:%v", conn.LocalAddr(), err)
		}
	}()
	reader := bufio.NewReader(conn)
	for {
		stringCh := make(chan []byte)
		errorCh := make(chan error)
		go func() {
			defer close(stringCh)
			defer close(errorCh)
			line, err := reader.ReadBytes('\n')
			if err != nil {
				errorCh <- err
			} else {
				stringCh <- line
			}
		}()
		select {
		case <-ctx.Done(): //不能因为ctx结束而导致有数据没有送进去从而导致缺失数据
			log.Debugf("[server based on Backward(listen on admin:%v)]The receiving server detects that ctx ends, terminates the forwarding and closes the channel", conn.LocalAddr())
			return
		case line := <-stringCh:
			line = bytes.TrimSuffix(line, []byte("\n"))
			log.Debugf("[server based on Backward(listen on admin:%v)]receive server receive data form connect %v->%v and will write to channel:%s", conn.LocalAddr(), conn.LocalAddr(), conn.RemoteAddr(), line)
			select {
			case <-ctx.Done():
				log.Debugf("[server based on Backward(listen on admin:%v)]The receiving server detects that ctx ends, terminates the forwarding and closes the channel", conn.LocalAddr())
				return
			case receiveChan <- line:
			}
		case err := <-errorCh:
			if err != nil {
				if err == io.EOF {
					log.Debugf("[server based on Backward(listen on admin:%v)]The connection is closed and the data read is complete", conn.LocalAddr())
					return
				}
				log.Errorf("[server based on Backward(listen on admin:%v)]ReceiveServer read from connect error:%v", conn.LocalAddr(), err.Error())
				return
			}
		}
	}
}

func (r *RemoteLoad) argServerHandle(ctx context.Context, conn net.Conn, arg []byte, log *golog.Logger) {
	defer func() {
		_ = conn.Close()
	}()
	buf := make([]byte, 30720)
	buffer := bytes.NewBuffer(arg)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := buffer.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Errorf("[server based on Backward(listen on admin:%v)]Description The parameter server failed to read parameters:%v", conn.LocalAddr().String(), err.Error())
			}
		}
		_, err = conn.Write(buf[:n])
		if err != nil {
			log.Errorf("[server based on Backward(listen on admin:%v)]The parameter server fails to write parameters into the connection:%v", conn.LocalAddr(), err.Error())
		}
	}
}

func (r *RemoteLoad) startListener(ctx context.Context, log *golog.Logger) (net.Listener, error) {
G:
	port, err := getlocaladdr.GetFreePortWithError(nil)
	if err != nil {
		return nil, err
	}
	log.Debugf("[server based on Backward]startServer on admin try port:%v", port)
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%v", port))
	if err != nil {
		if strings.Contains(err.Error(), "bind: address already in use") {
			log.Error("[server based on Backward]startServer listen error:%" + err.Error())
			time.Sleep(time.Millisecond * 500)
			goto G
		}
		return nil, err
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		}
	}()
	return listener, nil
}

// 在指定节点上开启一个连接lAddr的反向端口转发，并返回在agent上开启的端口号
func (r *RemoteLoad) startBackWard(ctx context.Context, uuid string, lAddr string, log *golog.Logger) (string, error) {
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
				log.Debugf("[server based on Backward(listen on admin:%v,agent:%v)]listen port %v on %v worn:already in use,start retry", lAddr, n, n, uuid)
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
	log.Debugf("[backward(listen on admin:%v,agent:%v(port:%v))]start success", lAddr, uuid, n)
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
	return nil, fmt.Errorf("did not match the standardized result of the module output：%v", re)
}
