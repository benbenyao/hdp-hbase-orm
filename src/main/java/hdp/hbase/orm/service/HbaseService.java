package hdp.hbase.orm.service;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;

import hdp.hbase.orm.bean.BuildingInfo;
import hdp.hbase.orm.dao.impl.BuildingInfoDaoImpl;

/**  
 * @Title: HbaseService.java
 * @Description: TODO(用一句话描述该文件做什么)
 * @author fengwei  
 * @date 2018年1月11日 上午11:43:18
 * @version V1.0  
 */
@Component
public class HbaseService {

	@Resource(name="buildingInfoDao")
	private BuildingInfoDaoImpl buildingInfoDao;
	
	public void test(){
		BuildingInfo buildingInfo = new BuildingInfo();
		buildingInfoDao.insert(buildingInfo);
	}
}
