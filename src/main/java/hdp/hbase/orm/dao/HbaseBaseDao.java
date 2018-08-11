package hdp.hbase.orm.dao;

import java.util.List;

import hdp.hbase.orm.pojo.HbaseBaseDo;

/**  
 * @Title: HbaseBaseDao.java
 * @Description: 简单接口
 * @author fengwei  
 * @date 2017年7月17日 下午5:31:06
 * @version V1.0  
 */
public interface HbaseBaseDao<T extends HbaseBaseDo> {
	public void insert(T stu);
	public void delete(String id);
	public void update(T stu);
	public T query(String id);
	
	/**
	 * @Title: scan  
	 * @Description: 查询全部
	 * @param @return    参数  
	 * @return List<T>    返回类型  
	 * @throws
	 */
	public List<T> scan();
}
