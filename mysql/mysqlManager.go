package mysql

import (
	"fmt"
	"sync"

	"github.com/xormplus/xorm"
)

type MysqlConnectManager struct {
	groups map[string]*xorm.EngineGroup
	lock   *sync.Mutex
}

var (
	mysqlManager *MysqlConnectManager
)

func init() {
	mysqlManager = &MysqlConnectManager{
		groups: make(map[string]*xorm.EngineGroup),
		lock:   new(sync.Mutex),
	}
}

// 保存初始化后的数据库引擎 xorm.Engine
func registerEngineGroup(dbname string, group *xorm.EngineGroup) {
	mysqlManager.lock.Lock()

	defer mysqlManager.lock.Unlock()

	mysqlManager.groups[dbname] = group
}

//提供给外部调用方法
func GetDB(dbname string) *xorm.EngineGroup {
	mysqlManager.lock.Lock()
	defer mysqlManager.lock.Unlock()

	if eg, ok := mysqlManager.groups[dbname]; ok {
		return eg
	}

	panic(fmt.Sprintf("%s Not Found", dbname))
}

func GetMasterDB(dbname string) *xorm.Engine {
	eg := GetDB(dbname)

	return eg.Master()
}

func GetSlaveDB(dbname string) *xorm.Engine {
	eg := GetDB(dbname)

	return eg.Slave()
}
