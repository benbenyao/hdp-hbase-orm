package hdp.hbase.orm;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hdp.hbase.orm.bean.BuildingInfo;

/**
 * @Title: HBaseUtils.java
 * @Description: 测试类
 * @author fengwei  
 * @date 2017年7月3日 下午2:55:01
 * @version V1.0
 */
public class HBaseUtils implements Serializable {

	private static final long serialVersionUID = 1L;

	private final static Logger logger = LoggerFactory.getLogger(HBaseUtils.class);

	// 创建hbase配置对象
	private static Configuration conf = null;
	private static ExecutorService pool = Executors.newScheduledThreadPool(5);
	private static Connection connection = null;
	private static HBaseUtils instance = null;

	private static final byte[] FAMILY1 = Bytes.toBytes("info");
	// private static final byte[] C1 = Bytes.toBytes("c1");

	static {
		conf = HBaseConfiguration.create();
		conf.addResource("hbase-site.xml");
		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public HBaseUtils() {
		if (connection == null || connection.isClosed()) {
			try {
				// 通过连接工厂创建连接对象
				// pool = Executors.newScheduledThreadPool(5);
				conf = HBaseConfiguration.create();
				conf.addResource("hbase-site.xml");
				// connection = ConnectionFactory.createConnection(conf, pool);
				connection = ConnectionFactory.createConnection(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static synchronized HBaseUtils getInstance() {
		if (instance == null) {
			instance = new HBaseUtils();
		}
		return instance;
	}

	public static synchronized Connection getConnection() {
		if (connection == null || connection.isClosed()) {
			try {
				conf = HBaseConfiguration.create();
				conf.addResource("hbase-site.xml");
				connection = ConnectionFactory.createConnection(conf, pool);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return connection;
	}

	/**
	 * 创建表
	 * 
	 * @throws IOException
	 */
	public void createTable(String tableName, String[] columns) throws IOException {
		// 得到管理程序
		Admin admin = connection.getAdmin();
		// 创建表名对象
		TableName name = TableName.valueOf(tableName);
		if (admin.tableExists(name)) {
			admin.disableTable(name);
			admin.deleteTable(name);
		} else {
			HTableDescriptor desc = admin.getTableDescriptor(name);
			for (String column : columns) {
				// 添加列族,每个表至少有一个列族.
				desc.addFamily(new HColumnDescriptor(column));
			}
			admin.createTable(desc);

		}
	}

	/**
	 * 插入一行记录
	 * 
	 * @param tablename
	 *            表名
	 * @param row
	 *            行名称
	 * @param columnFamily
	 *            列族名
	 * @param columns
	 *            （列族名：column）组合成列名
	 * @param values
	 *            行与列确定的值
	 */
	public void insertRecord(String tableName, String rowkey, List<byte[]> columns, List<byte[]> values) {
		this.insertRecord(tableName, rowkey, FAMILY1, columns, values);
	}

	/**
	 * 插入一行记录
	 * 
	 * @param tablename
	 *            表名
	 * @param row
	 *            行名称
	 * @param columnFamily
	 *            列族名
	 * @param columns
	 *            （列族名：column）组合成列名
	 * @param values
	 *            行与列确定的值
	 */
	public void insertRecord(String tableName, String rowkey, byte[] columnFamily, List<byte[]> columns,
			List<byte[]> values) {
		try {
			TableName name = TableName.valueOf(tableName);
			// 获得table对象
			Table table = connection.getTable(name);
			Put put = new Put(Bytes.toBytes(rowkey));
			for (int i = 0; i < columns.size(); i++) {
				put.addColumn(columnFamily, columns.get(i), values.get(i));
			}
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	/**
	 * 插入一行记录
	 * 
	 * @param tablename
	 *            表名
	 * @param row
	 *            行名称
	 * @param columnFamily
	 *            列族名
	 * @param columns
	 *            （列族名：column）组合成列名
	 * @param values
	 *            行与列确定的值
	 */
	public void insertRecord(String tableName, String rowkey, String columnFamily, String[] columns, String[] values) {
		try {
			TableName name = TableName.valueOf(tableName);
			// 获得table对象
			Table table = connection.getTable(name);
			Put put = new Put(Bytes.toBytes(rowkey));
			for (int i = 0; i < columns.length; i++) {
				put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(String.valueOf(columns[i])),
						Bytes.toBytes(values[i]));
				table.put(put);
				table.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 删除一行记录
	 * 
	 * @param tablename
	 *            表名
	 * @param rowkey
	 *            行名
	 * @throws IOException
	 */
	public void deleteRow(String tablename, String rowkey) throws IOException {
		TableName name = TableName.valueOf(tablename);
		Table table = connection.getTable(name);
		List<Delete> list = new ArrayList<Delete>();
		Delete d1 = new Delete(rowkey.getBytes());
		list.add(d1);
		table.delete(list);
	}

	/**
	 * 查找一行记录
	 * 
	 * @param tablename
	 *            表名
	 * @param rowkey
	 *            行名
	 */
	 public static void selectRow(String tablename, String rowKey) throws
	 IOException {
		 TableName name = TableName.valueOf(tablename);
		 Table table = connection.getTable(name);
		 Get g = new Get(rowKey.getBytes());
		 Result rs = table.get(g);
		 KeyValue[] keyVs = rs.raw();
		// for (KeyValue kv : keyVs) {//可以
		// byte[] qualifier = kv.getQualifier();
		// byte[] value = kv.getValue();
		// System.out.println(new String(qualifier) + "=" + new String(value));
		// }
		 for (Cell cell : rs.rawCells()) {
			// System.out.print(new String(cell.getRowArray()) + " ");
			// System.out.print(new String(cell.getFamilyArray()) + ":");
			// System.out.print(new String(cell.getQualifierArray()) + " ");
			// System.out.print(cell.getTimestamp() + " ");
			// System.out.println(new String(cell.getValueArray()));
			 String key = Bytes.toString(CellUtil.cloneRow(cell)); //取行键
			 long timestamp = cell.getTimestamp(); //取到时间戳
			 String family = Bytes.toString(CellUtil.cloneFamily(cell)); //取到族列
			 String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell)); //取到修饰名
			 String value = Bytes.toString(CellUtil.cloneValue(cell)); //取到值
			
			 System.out.println(" ===> rowKey : " + key + ", timestamp : " +
			 timestamp + ", family : " + family + ", qualifier : " + qualifier + ",value : " + value);
		 }
		 table.close();
	 }

	 public List<BuildingInfo> scanAllBuildingInfo(String tablename) throws Exception
	 {
		List<BuildingInfo> entityList = new ArrayList<BuildingInfo>();
		try {
			TableName name = TableName.valueOf(tablename);
			Table table = connection.getTable(name);
			Scan s = new Scan();
			// 得到扫描的结果集
			ResultScanner rs = table.getScanner(s);
			for (Result result : rs) {
				// 得到单元格集合
				List<Cell> cells = result.listCells();
				BuildingInfo entity = new BuildingInfo();
				Cell cell0 = cells.get(0);
				// 取行健
				entity.setRowKey(new String(CellUtil.cloneRow(cell0)));
				for (Cell cell : cells) {
					// 取到修饰名
					String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
					// 取到值
					String value = new String(CellUtil.cloneValue(cell), "gbk");
					if (qualifier.equals("latitude1")) {
						entity.setLatitude1(value);
					} else if (qualifier.equals("latitude2")) {
						entity.setLongitude2(value);
					}
				}
				entityList.add(entity);
			}
			table.close();
			return entityList;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return entityList;
	 }


	/**
	 * 删除表操作
	 * 
	 * @param tablename
	 * @throws IOException
	 */
	public void deleteTable(String tablename) throws IOException {
		try {
			TableName name = TableName.valueOf(tablename);
			// 得到管理程序
			Admin admin = connection.getAdmin();
			if (admin.tableExists(name)) {
				admin.disableTable(name);
				admin.deleteTable(name);
			}
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}
}
