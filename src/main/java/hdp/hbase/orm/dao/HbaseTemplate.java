package hdp.hbase.orm.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StopWatch;

import hdp.hbase.orm.region.RegionInfo;
import lombok.extern.slf4j.Slf4j;

/**
 * Central class for accessing the HBase API. Simplifies the use of HBase and helps to avoid common errors.
 * It executes core HBase workflow, leaving application code to invoke actions and extract results.
 *
 * @author Costin Leau
 * @author Shaun Elliott
 */
/**
 * JThink@JThink
 *
 * @author JThink
 * @version 0.0.1
 * desc： copy from spring data hadoop hbase, modified by JThink, use the 1.0.0 api
 * date： 2016-11-15 15:42:46
 */
@Slf4j
public class HbaseTemplate implements HbaseOperations {

    private Configuration configuration;

    private volatile Connection connection;

    public HbaseTemplate(Configuration configuration) {
        this.setConfiguration(configuration);
        Assert.notNull(configuration, " a valid configuration is required");
        /* 构造初始化 */
        getConnection();
    }

    @Override
    public <T> T execute(String tableName, TableCallback<T> action) throws Exception {
        Assert.notNull(action, "Callback object must not be null");
        Assert.notNull(tableName, "No table specified");

        StopWatch sw = new StopWatch();
        sw.start();
        Table table = null;
        try {
            table = this.getConnection().getTable(TableName.valueOf(tableName));
            return action.doInTable(table);
        } catch (Throwable throwable) {
        	throw new Exception(throwable);
        } finally {
            if (null != table) {
                try {
                    table.close();
                    sw.stop();
                } catch (IOException e) {
                    log.error("hbase资源释放失败");
                }
            }
        }
    }
    
//    @Override
//    public <T> List<T> find(String tableName, byte[] family, final RowMapper<T> action) throws Exception {
//        Scan scan = new Scan();
//        scan.setCaching(500);
//        scan.addFamily(family);
//        return this.find(tableName, scan, action);
//    }
//
//    @Override
//    public <T> List<T> find(String tableName, String family, final RowMapper<T> action) throws Exception {
//        Scan scan = new Scan();
//        scan.setCaching(500);
//        scan.addFamily(Bytes.toBytes(family));
//        return this.find(tableName, scan, action);
//    }
//
//    @Override
//    public <T> List<T> find(String tableName, String family, String qualifier, final RowMapper<T> action) throws Exception{
//        Scan scan = new Scan();
//        scan.setCaching(500);
//        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
//        return this.find(tableName, scan, action);
//    }
    
    

    @Override
    public <T> List<T> find(String tableName, final Scan scan, final RowMapper<T> action) throws Exception{
        return this.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(Table table) throws Throwable {
                int caching = scan.getCaching();
                // 如果caching未设置(默认是1)，将默认配置成500
                if (caching == 1) {
                    scan.setCaching(500);
                }
                ResultScanner scanner = table.getScanner(scan);
                try {
                    List<T> rs = new ArrayList<T>();
                    int rowNum = 0;
                    for (Result result : scanner) {
                        rs.add(action.mapRow(result, rowNum++));
                    }
                    return rs;
                } finally {
                    scanner.close();
                }
            }
        });
    }

    @Override
    public <T> T get(String tableName, String rowName, final RowMapper<T> mapper) throws Exception{
        return this.get(tableName, rowName, null, null, mapper);
    }

    @Override
    public <T> T get(String tableName, String rowName, String familyName, final RowMapper<T> mapper) throws Exception{
        return this.get(tableName, rowName, familyName, null, mapper);
    }

    @Override
    public <T> T get(String tableName, final String rowName, final String familyName, final String qualifier, final RowMapper<T> mapper) throws Exception{
        return this.execute(tableName, new TableCallback<T>() {
            @Override
            public T doInTable(Table table) throws Throwable {
                Get get = new Get(Bytes.toBytes(rowName));
                if (StringUtils.isNotBlank(familyName)) {
                    byte[] family = Bytes.toBytes(familyName);
                    if (StringUtils.isNotBlank(qualifier)) {
                        get.addColumn(family, Bytes.toBytes(qualifier));
                    }
                    else {
                        get.addFamily(family);
                    }
                }
                Result result = table.get(get);
                return mapper.mapRow(result, 0);
            }
        });
    }
    
    @Override
    public <T> List<T> get(String tableName, final List<String> rowNames, final byte[] family, 
    		final List<String> qualifiers, final RowMapper<T> action) throws Exception{
        return this.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(Table table) throws Throwable {
            	List<Get> gets = new ArrayList<Get>(rowNames.size());
            	for(String rowName: rowNames){
            		 Get get = new Get(Bytes.toBytes(rowName));
            		 if (family!=null) {
                         if (qualifiers!=null && qualifiers.size()>0) {
                         	for(String qualifier : qualifiers){
                         		get.addColumn(family, Bytes.toBytes(qualifier));
                         	}
                         } else {
                             get.addFamily(family);
                         }
                     }
            		 
            		 gets.add(get);
            	}
                
            	Result[] results = table.get(gets);
            	
                List<T> rs = new ArrayList<T>();
                int rowNum = 0;
                for (Result result : results) {
                    rs.add(action.mapRow(result, rowNum++));
                }
            	
                return rs;
            }
        });
    }
    
    @Override
    public <T> List<T> get(String tableName, final String[] rowNames, final byte[] family, 
    		final List<String> qualifiers, final RowMapper<T> action) throws Exception{
        return this.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(Table table) throws Throwable {
            	List<Get> gets = new ArrayList<Get>(rowNames.length);
            	for(String rowName: rowNames){
            		 Get get = new Get(Bytes.toBytes(rowName));
            		 if (family!=null) {
                         if (qualifiers!=null && qualifiers.size()>0) {
                         	for(String qualifier : qualifiers){
                         		get.addColumn(family, Bytes.toBytes(qualifier));
                         	}
                         } else {
                             get.addFamily(family);
                         }
                     }
            		 
            		 gets.add(get);
            	}
                
            	Result[] results = table.get(gets);
            	
                List<T> rs = new ArrayList<T>();
                int rowNum = 0;
                for (Result result : results) {
                    rs.add(action.mapRow(result, rowNum++));
                }
            	
                return rs;
            }
        });
    }
    
    @Override
    public <T> List<T> get(String tableName, final List<String> rowNames, final String familyName, 
    		final List<String> qualifiers, final RowMapper<T> action) throws Exception{
        return this.execute(tableName, new TableCallback<List<T>>() {
            @Override
            public List<T> doInTable(Table table) throws Throwable {
            	List<Get> gets = new ArrayList<Get>(rowNames.size());
            	for(String rowName: rowNames){
            		 Get get = new Get(Bytes.toBytes(rowName));
            		 if (StringUtils.isNotBlank(familyName)) {
                         byte[] family = Bytes.toBytes(familyName);
                         if (qualifiers!=null && qualifiers.size()>0) {
                         	for(String qualifier : qualifiers){
                         		get.addColumn(family, Bytes.toBytes(qualifier));
                         	}
                         } else {
                             get.addFamily(family);
                         }
                     }
            		 
            		 gets.add(get);
            	}
                
            	Result[] results = table.get(gets);
            	
                List<T> rs = new ArrayList<T>();
                int rowNum = 0;
                for (Result result : results) {
                    rs.add(action.mapRow(result, rowNum++));
                }
            	
                return rs;
            }
        });
    }
    
    

    @Override
    public void execute(String tableName, MutatorCallback action) throws Exception{
        Assert.notNull(action, "Callback object must not be null");
        Assert.notNull(tableName, "No table specified");

        StopWatch sw = new StopWatch();
        sw.start();
        BufferedMutator mutator = null;
        try {
            BufferedMutatorParams mutatorParams = new BufferedMutatorParams(TableName.valueOf(tableName));
            mutator = this.getConnection().getBufferedMutator(mutatorParams.writeBufferSize(32 * 1024 * 1024));
            action.doInMutator(mutator);
        } catch (Throwable throwable) {
            sw.stop();
            throw new Exception(throwable);
        } finally {
            if (null != mutator) {
                try {
                    mutator.flush();
                    mutator.close();
                    sw.stop();
                } catch (IOException e) {
                    log.error("hbase mutator资源释放失败");
                }
            }
        }
    }

    /**
     * 单个保存
     */
    @Override
    public void saveOrUpdate(String tableName, final Mutation mutation) throws Exception{
        this.execute(tableName, new MutatorCallback() {
            @Override
            public void doInMutator(BufferedMutator mutator) throws Throwable {
                mutator.mutate(mutation);
            }
        });
    }

    /**
     * 批量保存
     */
    @Override
    public void saveOrUpdates(String tableName, final List<Mutation> mutations) throws Exception{
        this.execute(tableName, new MutatorCallback() {
            @Override
            public void doInMutator(BufferedMutator mutator) throws Throwable {
                mutator.mutate(mutations);
            }
        });
    }
    
    /**
     * 批量保存
     */
    @Override
    public void saves(String tableName, final List<Mutation> mutations) throws Exception{
        this.execute(tableName, new MutatorCallback() {
            @Override
            public void doInMutator(BufferedMutator mutator) throws Throwable {
                mutator.mutate(mutations);
            }
        });
    }
    
    /**
     * @Title: regionNum  
     * @Description: 获取某个表的分区数  
     * @param @return    参数  
     * @return int    返回类型  
     * @throws
     */
    public List<RegionInfo> getRegionInfo(String tablename) throws Exception{
    	try {
    		RegionLocator regionLocator = this.getConnection().getRegionLocator(TableName.valueOf(tablename));
//    		Pair<byte[][], byte[][]> startEndKeys = regionLocator.getStartEndKeys();
    		
    		List<HRegionLocation> allRegionLocations = regionLocator.getAllRegionLocations();
    		List<RegionInfo> regionInfos = new ArrayList<RegionInfo>(allRegionLocations.size());
    		
    		for(HRegionLocation hregionLocation : allRegionLocations){
    			RegionInfo regionInfo = new RegionInfo();
    			regionInfo.setStartKey(new String(hregionLocation.getRegionInfo().getStartKey()));
    			regionInfo.setEndKey(new String(hregionLocation.getRegionInfo().getEndKey()));
    			regionInfos.add(regionInfo);
    		}
    		
    		return regionInfos;
    		
		} catch (Exception e) {
			log.error("regionNum-error{}", e);
			return null;
		}
    }
    
    /**
     * @Title: locationRegion  
     * @Description: 定位region  
     * @param @param tablename
     * @param @param rowkey    参数  
     * @return void    返回类型  
     * @throws
     */
    public HRegionLocation relocateRegion(String tablename, String rowkey){
    	HRegionLocation location = null;
    	try {
    		RegionLocator regionLocator = this.getConnection().getRegionLocator(TableName.valueOf(tablename));
    		location = regionLocator.getRegionLocation(Bytes.toBytes(rowkey));
    		
	    } catch (Exception e) {
			log.error("regionNum-error{}", e);
		}
    	return location;
    }
    
    /**
     * @Title: regionNum  
     * @Description: 获取某个表的分区数  
     * @param @return    参数  
     * @return int    返回类型  
     * @throws
     */
    public int regionNum(String tablename) throws Exception{
    	try {
    		RegionLocator regionLocator = this.getConnection().getRegionLocator(TableName.valueOf(tablename));
    		
    		return regionLocator.getAllRegionLocations().size();
    		
		} catch (Exception e) {
			log.error("regionNum-error{}", e);
			return 0;
		}
    }
    
    /**
     * @Title: regionOperation  
     * @Description: 查询分区信息，测试用
     * @param     参数  
     * @return void    返回类型  
     * @throws
     */
    public void regionOperation(String tableName) throws Exception{
    	
    	try {
    		RegionLocator regionLocator = this.getConnection().getRegionLocator(TableName.valueOf(tableName));
    		
    		List<HRegionLocation> allRegionLocations = regionLocator.getAllRegionLocations();
    		for (HRegionLocation hRegionLocation : allRegionLocations) {
    			System.out.println(hRegionLocation);
    			HRegionInfo regionInfo = hRegionLocation.getRegionInfo();//region=FaceCharact,,1514442503980.81bdf68f3942404310c2e2a593743e78., hostname=mongodbb,16020,1514358721743, seqNum=-1
    			System.out.println(regionInfo);//{ENCODED => 81bdf68f3942404310c2e2a593743e78, NAME => 'FaceCharact,,1514442503980.81bdf68f3942404310c2e2a593743e78.', STARTKEY => '', ENDKEY => ''}
    		}
    		
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
    
    
    public int countData(String tableName) throws Exception {
		long start = System.currentTimeMillis();
		Scan scan = new Scan();
		Table table = null;
	    try {
	    	scan.setFilter(new FirstKeyOnlyFilter());
	    	table = this.getConnection().getTable(TableName.valueOf(tableName));
	    	ResultScanner scanner = table.getScanner(scan);
			int i=0;
			for (Result result : scanner) {
				i++;
			}
			long stop = System.currentTimeMillis();
			log.error("计数"+i+"\t计数花费"+(stop-start)/1000+"秒");
			return i;
	    } catch (Exception e) {
			 log.error("countData失败{}",e);
			 return 0;
		}
	}

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Connection getConnection() {
        if (null == this.connection) {
            synchronized (this) {
                if (null == this.connection) {
                    try {
//                        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(10, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
                    	ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(2, 8, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
                    	// init pool
                        poolExecutor.prestartCoreThread();
                        this.connection = ConnectionFactory.createConnection(configuration, poolExecutor);
                    } catch (IOException e) {
                        log.error("hbase connection资源池创建失败");
                    }
                }
            }
        }
        return this.connection;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }
    
}
