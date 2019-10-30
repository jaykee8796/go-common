package mysql

import (
	"fmt"

	"gitee.taojiji.com/micro/infrastructure/utils"
)

//使用默认 一致性hash环 分库，直接调用此方法即可
func ConsistentHashShardingFunc(replicas int) MysqlShardingFunc {
	var (
		ct           *utils.Consistent
		shardingMaps = make(map[string]int)
	)
	return func(mysqlConfig MysqlConfig, key interface{}) (id int, err error) {
		if ct == nil {
			ct = utils.NewConsistent()
			ct.NumberOfReplicas = replicas
			//init
			for _, conf := range mysqlConfig.Instances {
				ct.Add(conf.InstanceName)
				shardingMaps[conf.InstanceName] = conf.DBID
			}
		}

		v, err := ct.Get(fmt.Sprint(key))

		if err != nil {
			return 0, err
		}

		if id, ok := shardingMaps[v]; ok {
			return id, nil
		}

		return 0, err
	}
}
