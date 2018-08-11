package hdp.hbase.orm.pojo;

import org.springframework.data.annotation.Id;

/**  
 * @Title: HbaseBase.java
 * @Description: 抽象rowKey
 * @author fengwei  
 * @date 2017年7月17日 下午5:24:22
 * @version V1.0  
 */
public abstract class HbaseBaseDo {
	
	@Id
	protected String rowKey;
	
    public abstract String getRowKey();
    
    /**
     * @Title: setRowKey  
     * @Description: 在rowkey不属于实体的一部分时，将rowkey赋予id
     * @param @param rowkey    参数  
     * @return void    返回类型  
     * @throws
     */
    public abstract void setRowKey(String rowkey);

    public byte[] rowKeyBytes() {
        return getRowKey() == null ? null : getRowKey().getBytes();
    }

    public boolean hasRowKey() {
        return getRowKey() != null && !getRowKey().isEmpty();
    }

}
