package remote_load

// ModuleBaseInfo 以json的格式放在模块的同级目录下面
type ModuleBaseInfo struct {
	Name            string   `json:"name"`      //
	Introduce       string   `json:"introduce"` //模块介绍
	FileName        string   `json:"file_name"` //模块的文件名
	SupportedSystem []string `json:"supported_system"`
	Version         string   `json:"version"` //模块版本
}

// ModuleInfo 模块的基本信息
type ModuleInfo struct {
	*ModuleBaseInfo

	ModulePath string `json:"module_path"` //模块的具体路径及文件名
}

func (m *ModuleInfo) GetInfo() *ModuleInfo {
	return m
}
