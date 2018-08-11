package hdp.hbase.orm.dao.impl;

import java.io.Serializable;

import org.springframework.stereotype.Component;

import hdp.hbase.orm.bean.BuildingInfo;
import hdp.hbase.orm.support.HbaseDaoSupport;

/**  
 * @Title: BuildingInfoDaoImpl.java
 * @Description: TODO(用一句话描述该文件做什么)
 * @author fengwei  
 * @date 2017年7月25日 上午2:23:04
 * @version V1.0  
 */
@Component("buildingInfoDao")
public class BuildingInfoDaoImpl  extends HbaseDaoSupport<BuildingInfo> implements Serializable{
	private static final long serialVersionUID = 1L;
	
	public static final String tableName = "building_info";
	
	public BuildingInfoDaoImpl() {
		super(tableName, BuildingInfo.class);
	}
}
