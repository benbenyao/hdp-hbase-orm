package hdp.hbase.orm.bean;


import com.alibaba.fastjson.annotation.JSONField;

import hdp.hbase.orm.annotation.HBColumn;
import hdp.hbase.orm.pojo.HbaseBaseDo;

/**  
 * @Title: BuildingInfo.java
 * @Description: 实体bean
 * @author fengwei  
 * @date 2017年6月28日 下午5:35:22
 * @version V1.0  
 */
public class BuildingInfo extends HbaseBaseDo implements java.io.Serializable{
	
	private static final long serialVersionUID = 1L;
	
	@JSONField(name = "latitude1")
	@HBColumn(family = "info", qualifier = "latitude1")
	private String latitude1;
	
	@JSONField(name = "longitude2")
	@HBColumn(family = "info", qualifier = "longitude2")
	private String longitude2;
	
	@Override
	public String getRowKey() {
		return rowKey;
	}
	
	@Override
	public void setRowKey(String rowkey) {
		this.rowKey = rowkey;
	}
	
	public String getLatitude1() {
		return latitude1;
	}

	public void setLatitude1(String latitude1) {
		this.latitude1 = latitude1;
	}

	public String getLongitude2() {
		return longitude2;
	}

	public void setLongitude2(String longitude2) {
		this.longitude2 = longitude2;
	}

}
