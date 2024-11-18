package moduleArgs

// 程序架构，目前支持arm和x86
const (
	Arm = "arm"
	X86 = "x86"
)

type ArgWithType struct {
	ArgType int
	Arg     []byte
}

// 远程加载模块传输的请求参数类型
const (
	ArgsAddr = iota + 1
	Args
	PushResultAddr  //往服务端推送数据
	ReceiveDataAddr //服务端往模块推送数据（暂未开放）
	OnlyBackward    //仅仅开启一个远程端口转发给模块访问server的服务
)

// ModeRequest 所有模块接收到的数据结构
type ModeRequest []RequestInfo

type RequestInfo struct {
	Type int    `json:"type"` //请求参数类型,标记了参数的传递方式（直接传输、地址传输）
	Addr string `json:"addr"` //地址，一般为127.0.0.1:port
	Args []byte `json:"args"` //参数
}

// ModeResponse 所有模块返回的数据结构
type ModeResponse struct {
	ModuleName string `json:"module_name"`
	Data       []byte `json:"data"` //执行结果的序列化内容
	Error      string `json:"error"`
	Time       string `json:"time"`
}
