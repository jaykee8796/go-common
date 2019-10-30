package mysql

import (
	"fmt"
	"sync"

	"github.com/xormplus/xorm"
)

//自定义分库hash算法
type MysqlShardingFunc func(mysqlConfig MysqlConfig, key interface{}) (id int, err error)

type MysqlSharding struct {
	dbs          map[int]*xorm.EngineGroup //master and slave   dbid => engine
	shardingFunc MysqlShardingFunc
	mysqlConfig  MysqlConfig
	lock         *sync.Mutex
}

type MysqlShardingManager struct {
	groups map[string]*MysqlSharding
	lock   *sync.Mutex
}

var shardingManager *MysqlShardingManager

func init() {
	shardingManager = &MysqlShardingManager{
		groups: make(map[string]*MysqlSharding),
		lock:   new(sync.Mutex),
	}
}

func registerMysqlShardingDatabase(dbname string, sharing *MysqlSharding) {
	shardingManager.lock.Lock()
	defer shardingManager.lock.Unlock()

	shardingManager.groups[dbname] = sharing
}

func GetShardingDB(dbname string, key interface{}) *xorm.EngineGroup {
	shardingManager.lock.Lock()
	defer shardingManager.lock.Unlock()

	//call func
	var (
		cluster *MysqlSharding
		ok      bool
	)
	if cluster, ok = shardingManager.groups[dbname]; !ok {
		panic(fmt.Sprintf("%s Not Found", dbname))
	}

	id, err := cluster.shardingFunc(cluster.mysqlConfig, key)

	if err != nil {
		panic(err)
	}

	cluster.lock.Lock()
	defer cluster.lock.Unlock()

	if eg, ok := cluster.dbs[id]; ok {
		return eg
	}

	return nil
}

func GetShardingMasterDB(dbname string, key interface{}) *xorm.Engine {
	return GetShardingDB(dbname, key).Master()
}

func GetShardingSlaveDB(dbname string, key interface{}) *xorm.Engine {
	return GetShardingDB(dbname, key).Slave()
}

func GetShardingSubDatases(dbname string) map[int]*xorm.EngineGroup {
	shardingManager.lock.Lock()
	defer shardingManager.lock.Unlock()

	if cluster, ok := shardingManager.groups[dbname]; ok {
		return cluster.dbs
	}

	panic(fmt.Sprintf("%s Not Found", dbname))
}
