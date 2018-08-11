package hdp.hbase.orm.region;

import lombok.Data;

/**  
 * @Title: RegionInfo.java
 * @Description: TODO(用一句话描述该文件做什么)
 * @author fengwei  
 * @date 2018年1月3日 下午5:36:47
 * @version V1.0  
 */
@Data
public class RegionInfo {
	
	private String startKey;
	
	private String endKey;
	
}
