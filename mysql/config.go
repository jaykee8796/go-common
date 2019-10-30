package mysql

import (
	"errors"
	"fmt"
	"gitee.taojiji.com/micro/infrastructure/ioutil"
	"sync"
	"time"

	"github.com/xormplus/xorm"

	"gopkg.in/yaml.v2"

	_ "github.com/go-sql-driver/mysql"
)

const (
	//[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	//https://godoc.org/github.com/go-sql-driver/mysql#Config.FormatDSN
	//ConnStrTmpl = "%s:%s@tcp(%s:%s)/%s?charset=%s&loc=%s&parseTime=True"
	ConnStrTmpl = "%s:%s@tcp(%s:%s)/%s?charset=%s&parseTime=True"
	DefaultLOC  = "Asia%2FShanghai"

	DefaultMaxOpenConns = 200
	DefaultMaxIdleConns = 60
	DefaultMaxLeftTime  = 300 * time.Second
)

var (
	defaultDatabase = "mysql"
	apolloAppID     = "conn.mysql"
	apolloPrefix    = "mysql"
	consulPrefix    = "conn/mysql"
)

//每个数据库节点配置
type MysqlNode struct {
	InstanceName string `yaml:"name"`
	Server       string `yaml:"server"`
	Port         string `yaml:"port"`
	UserID       string `yaml:"user"`
	Password     string `yaml:"password"`
	IsMaster     bool   `yaml:"isMaster"`
	Weight       int    `yaml:"weight"`
	DB           string `yaml:"dbname"`
	DBID         int    `yaml:"dbid"`
}

//数据yaml配置，格式为：
//
//name: shardingdb
//charset: utf8
//maxActive: 50
//minIdle: 10
//maxWaitTime: 10
//isSharding: true
//instances:
//- name: DB_CONFIG_TAOJJ_1
//  server : 192.168.30.20
//  port : 3306
//  user : db_dev_curd
//  password : "DB_CURD_7hRLVxSp9pF"
//  isMaster : true
//  weight: 50
//  dbname: db1
//  dbid: 0
//- name: DB_CONFIG_TAOJJ_2
//  server : 192.168.30.20
//  port : 3306
//  user : db_dev_curd
//  password : "DB_CURD_7hRLVxSp9pF"
//  isMaster : true
//  weight: 10
//  dbname: db2
//  dbid: 1
//- name: DB_CONFIG_TAOJJ_3
//  server : 192.168.30.20
//  port : 3306
//  user : db_dev_curd
//  password : "DB_CURD_7hRLVxSp9pF"
//  isMaster : true
//  weight: 10
//  dbname: db3
//  dbid: 2
type MysqlConfig struct {
	Name        string      `yaml:"name"`
	CharSet     string      `yaml:"charset"`
	MaxActive   int         `yaml:"maxActive"`
	MinIdle     int         `yaml:"minIdle"`
	MaxWaitTime int         `yaml:"maxWaitTime"`
	IsSharding  bool        `yaml:"isSharding"`
	Instances   []MysqlNode `yaml:"instances"`
}

func getMysqlConfig(filename string) (config MysqlConfig, err error) {
	buf, err := ioutil.ReadAllBytes(filename)
	if (err != nil) {
		return config, err
	}
	if err = yaml.Unmarshal(buf, &config); err != nil {
		return config, err
	}

	return config, nil
}

func LoadConfig(filenames ...string) ([]MysqlConfig, error) {
	if len(filenames) == 0 {
		return nil, errors.New("You must provider at least a name of db")
	}

	var result []MysqlConfig
	for _, dbname := range filenames {
		config, err := getMysqlConfig(dbname)
		if err != nil {
			return nil, err
		}

		if config.IsSharding {
			return nil, fmt.Errorf(fmt.Sprintf("Current database: %v is sharding database, please use LoadShardingConfig.", dbname))
		}

		if err = registerDatabase(dbname, config); err != nil {
			return nil, err
		}
		result = append(result, config)
	}
	return result, nil
}

func registerShardingDatabase(name string, config MysqlConfig, shardingFunc MysqlShardingFunc) (err error) {
	var charset string
	if config.CharSet == "" {
		charset = "utf8"
	} else {
		charset = config.CharSet
	}

	var (
		masterEngine  *xorm.Engine
		slaveEngine   *xorm.Engine
		dbs           = make(map[int]*xorm.EngineGroup)
		masterEngines = make(map[int]*xorm.Engine)
		slaveEngines  = make(map[int][]*xorm.Engine)
		weight        = make(map[int][]int)
		connStr       string
		dbId          int
		eg            *xorm.EngineGroup
	)

	maxIdleConns := config.MinIdle
	maxOpenConns := config.MaxActive
	maxLifeTime := time.Duration(config.MaxWaitTime) * time.Second

	//if maxIdleConns == 0 || maxIdleConns > DefaultMaxIdleConns {
	maxIdleConns = DefaultMaxIdleConns
	//}

	//if maxOpenConns == 0 || maxOpenConns > DefaultMaxOpenConns {
	maxOpenConns = DefaultMaxOpenConns
	//}

	if maxLifeTime <= 0 {
		maxLifeTime = DefaultMaxLeftTime
	}

	if len(config.Instances) == 0 {
		return fmt.Errorf("The db(%s)'s configure is invalid, Because the instances is empty", name)
	}

	for _, instance := range config.Instances {
		connStr = fmt.Sprintf(ConnStrTmpl, instance.UserID, instance.Password, instance.Server, instance.Port, instance.DB, charset)
		dbId = instance.DBID
		if instance.IsMaster {
			if _, ok := masterEngines[dbId]; !ok {
				if masterEngine, err = xorm.NewEngine("mysql", connStr); err != nil {
					return err
				}

				masterEngine.SetMaxIdleConns(maxIdleConns)
				masterEngine.SetMaxOpenConns(maxOpenConns)
				masterEngine.SetConnMaxLifetime(maxLifeTime)

				masterEngines[dbId] = masterEngine
			}
		} else {
			if slaveEngine, err = xorm.NewEngine("mysql", connStr); err != nil {
				return err
			}
			slaveEngine.SetMaxIdleConns(maxIdleConns)
			slaveEngine.SetMaxOpenConns(maxOpenConns)
			slaveEngine.SetConnMaxLifetime(maxLifeTime)

			weight[dbId] = append(weight[dbId], instance.Weight)
			slaveEngines[dbId] = append(slaveEngines[dbId], slaveEngine)
		}
	}

	for dbId := range masterEngines {
		if eg, err = xorm.NewEngineGroup(masterEngines[dbId], slaveEngines[dbId], xorm.WeightRandomPolicy(weight[dbId])); err != nil {
			return err
		}

		dbs[dbId] = eg
	}

	registerMysqlShardingDatabase(name, &MysqlSharding{
		dbs:          dbs,
		shardingFunc: shardingFunc,
		mysqlConfig:  config,
		lock:         new(sync.Mutex),
	})

	return nil
}

func registerDatabase(name string, config MysqlConfig) error {
	var charset string
	if config.CharSet == "" {
		charset = "utf8"
	} else {
		charset = config.CharSet
	}

	var (
		masterEngine *xorm.Engine
		slaveEngine  *xorm.Engine
		slaveEngines = make([]*xorm.Engine, 0)
		weight       = make([]int, 0)
		connStr      string
		err          error
		eg           *xorm.EngineGroup
	)

	maxIdleConns := config.MinIdle
	maxOpenConns := config.MaxActive
	maxLifeTime := time.Duration(config.MaxWaitTime) * time.Second

	//if maxIdleConns == 0 || maxIdleConns > DefaultMaxIdleConns {
	maxIdleConns = DefaultMaxIdleConns
	//}

	//if maxOpenConns == 0 || maxOpenConns > DefaultMaxOpenConns {
	maxOpenConns = DefaultMaxOpenConns
	//}

	if maxLifeTime <= 0 {
		maxLifeTime = DefaultMaxLeftTime
	}

	if len(config.Instances) == 0 {
		return fmt.Errorf("The db(%s)'s configure is invalid, Because the instances is empty", name)
	}

	for _, instance := range config.Instances {
		// 在非sharding情况下，master只会有一个
		connStr = fmt.Sprintf(ConnStrTmpl, instance.UserID, instance.Password, instance.Server, instance.Port, instance.DB, charset)
		if instance.IsMaster && masterEngine == nil {
			if masterEngine, err = xorm.NewEngine("mysql", connStr); err != nil {
				return err
			}
			masterEngine.SetMaxIdleConns(maxIdleConns)
			masterEngine.SetMaxOpenConns(maxOpenConns)
			masterEngine.SetConnMaxLifetime(maxLifeTime)

		} else {
			if slaveEngine, err = xorm.NewEngine("mysql", connStr); err != nil {
				return err
			}
			slaveEngine.SetMaxIdleConns(maxIdleConns)
			slaveEngine.SetMaxOpenConns(maxOpenConns)
			slaveEngine.SetConnMaxLifetime(maxLifeTime)

			//获取权重数据
			weight = append(weight, instance.Weight)
			slaveEngines = append(slaveEngines, slaveEngine)
		}
	}

	if eg, err = xorm.NewEngineGroup(masterEngine, slaveEngines, xorm.WeightRandomPolicy(weight)); err != nil {
		return err
	}

	registerEngineGroup(name, eg)

	return nil
}

//如果使用GetMysqlDefaultDB 那必须要调用SetDefaultdatabase设定默认数据库
func SetDefaultdatabase(name string) {
	defaultDatabase = name
}

func GetMysqlDefaultDB() *xorm.EngineGroup {
	return GetDB(defaultDatabase)
}
