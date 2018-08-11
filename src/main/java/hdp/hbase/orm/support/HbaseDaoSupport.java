package hdp.hbase.orm.support;

import java.io.IOException;
import java.lang.annotation.IncompleteAnnotationException;
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TimeZone;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

import hdp.hbase.orm.HBaseUtils;
import hdp.hbase.orm.annotation.HBColumn;
import hdp.hbase.orm.annotation.JsonField;
import hdp.hbase.orm.annotation.MapField;
import hdp.hbase.orm.pojo.HbaseBaseDo;
import hdp.hbase.orm.util.ReflectionUtil;

/**
 * @Title: HbaseDaoSupport.java
 * @Description: 基本操作，基于反射实现
 * @author fengwei  
 * @date 2017年7月17日 下午4:27:26
 * @version V1.0
 * 
 * 这个类的基本数据类型转化基本用不上，因为有数据是用hbase的ImportTsv导入的，数据类型为String转为byte[]，所以代码层面也要适应
 */
public class HbaseDaoSupport<T extends HbaseBaseDo>{

	private final static Logger LOG = LoggerFactory.getLogger(HbaseDaoSupport.class);
	
	public static TimeZone defaultTimeZone = TimeZone.getDefault();
	public static Locale defaultLocale = Locale.getDefault();
    private String dateFormatPattern;
    private DateFormat dateFormat;

    private String tableName;
    private Class<?> clz;

    public HbaseDaoSupport() {
	}

	public HbaseDaoSupport(String tableName, Class<?> clz) {
        this.tableName = tableName;
        this.clz = clz;
    }
    
    /**
	 * @Title: scan  
	 * @Description: 查询全部
	 * @param @return    参数  
	 * @return List<T>    返回类型  
	 * @throws
	 */
    public List<T> scan(){
    	Connection connection = null;
        Table table = null;
        List<T> list = new ArrayList<T>();
        try {
            connection = HBaseUtils.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            Scan s = new Scan();
			 //得到扫描的结果集
			 ResultScanner rs = table.getScanner(s);
			 for(Result result : rs){
		         T t = buildDo(result);
		         list.add(t);
			 }
        } catch (Exception e) {
            LOG.error("some error", e);
        } finally {
        	closeTable(table);
        }
        return list;
    }

    public void insert(T obj) {
        Connection connection = null;
        Table table = null;
        if (!obj.hasRowKey()) {
            return;
        }
        try {
            connection = HBaseUtils.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            table.put(doPut(obj));
        } catch (IOException e) {
            LOG.error("some error", e);
        } catch (IllegalAccessException e) {
            LOG.error("some error", e);
        } finally {
//            closeAll(table, connection);
        	closeTable(table);
        }
    }
    
    public void insertBatch(List<T> objs) {
        Connection connection = null;
        Table table = null;
        try {
            connection = HBaseUtils.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            List<Put> puts = new ArrayList<Put>();
            for(T obj: objs){
            	 if (!obj.hasRowKey()) {
                     continue;
                 }
            	 puts.add(doPut(obj));
            }
            table.put(puts);
        } catch (Exception e) {
            LOG.error("some error", e);
        } finally {
        	closeTable(table);
        }
    }

    public void delete(String rowkey) {
        if (rowkey == null || rowkey.isEmpty()) {
            return;
        }
        Connection connection = null;
        Table table = null;
        try {
            connection = HBaseUtils.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(rowkey.getBytes());
            table.delete(del);
        } catch (IOException e) {
            LOG.error("some error", e);
        } finally {
            closeTable(table);
        }
    }
    
    public void deleteList(List<String> rowkeys) {
        Connection connection = null;
        Table table = null;
        try {
            connection = HBaseUtils.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            List<Delete> deletes = new ArrayList<Delete>();
            for(String rowkey: rowkeys){
            	Delete del = new Delete(rowkey.getBytes());
            	deletes.add(del);
            }
            
            table.delete(deletes);
        } catch (IOException e) {
            LOG.error("some error", e);
        } finally {
            closeTable(table);
        }
    }
    
    /**
     * @Title: scanByPrefixFilter  
     * @Description: 进行前缀相同检索过滤
     * @param @param tablename
     * @param @param rowPrifix    参数  
     * @return void    返回类型  
     * @throws
     */
    public List<String> scanByPrefixFilter(String rowPrifix) {
    	 Connection connection = null;
         Table table = null;
         List<String> rowkeys = new ArrayList<String>();
	     try {
	    	   connection = HBaseUtils.getConnection();
	           table = connection.getTable(TableName.valueOf(tableName));
	           Scan s = new Scan();
	           s.setFilter(new PrefixFilter(rowPrifix.getBytes()));
	           ResultScanner rs = table.getScanner(s);
	           for (Result r : rs) {
	        	   rowkeys.add(new String(r.getRow()));
	           }
	           return rowkeys;
	      } catch (IOException e) {
	          LOG.error("some error", e);
	      } finally {
	          closeTable(table);
	      }
		return rowkeys;
    }

    public void update(T obj) {
        if (!obj.hasRowKey()) {
            return;
        }

        Connection connection = null;
        Table table = null;
        try {
            connection = HBaseUtils.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            if(table.exists(new Get(obj.rowKeyBytes()))){
            	 long now = System.currentTimeMillis();
            	 Delete del = new Delete(obj.rowKeyBytes(), now);
                 Put put = new Put(obj.rowKeyBytes(), now + 1);
                 Field[] fields = ReflectionUtil.getDeclaredFields(obj);
                 for (Field field : fields) {
                    field.setAccessible(true);
                    if(!columnAndFieldNullCheck(obj, field)){//不处理没有注解的
                    	continue;
                    }
	               	pojoPut(put, obj, field);
                 }
                 //HBASE-8626
                 RowMutations rowMutations = new RowMutations(obj.rowKeyBytes());
                 rowMutations.add(del);
                 rowMutations.add(put);
                 table.mutateRow(rowMutations);
            } else {
            	 table.put(doPut(obj));
            }
        } catch (Exception e) {
            LOG.error("connection error when update", e);
        } finally {
        	closeTable(table);
        }
    }

    /*
    public void update(T obj) {
        if (!obj.hasRowKey()) {
            return;
        }

        Connection connection = null;
        Table table = null;
        long now = System.currentTimeMillis();
        try {
            connection = HBaseUtils.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(obj.rowKeyBytes(), now);
            Put put = new Put(obj.rowKeyBytes(), now + 1);
            Field[] fields = ReflectionUtil.getDeclaredFields(obj);
            for (Field field : fields) {
                field.setAccessible(true);
                if(!columnCheck(field)){//不处理没有注解的
            		continue;
                }
                if (field.isAnnotationPresent(JsonField.class)) {
                    jsonBytesPut(put, obj, field);
                } else if (field.isAnnotationPresent(MapField.class)) {
                    del.addFamily(field.getAnnotation(HBColumn.class).family().getBytes());
                    mapPut(put, obj, field);
                } else {
                    pojoPut(put, obj, field);
                }
            }
            //HBASE-8626
            RowMutations rowMutations = new RowMutations(obj.rowKeyBytes());
            rowMutations.add(del);
            rowMutations.add(put);
            table.mutateRow(rowMutations);
        } catch (IllegalAccessException e) {
            LOG.error("put error when update", e);
        } catch (IOException e) {
            LOG.error("connection error when update", e);
        } finally {
        	closeTable(table);
        }
    }
    */

    public T query(String rowkey) {
        if (rowkey == null || rowkey.isEmpty()) {
            return null;
        }
        Get get = new Get(rowkey.getBytes());
        Connection connection = null;
        Table table = null;
        try {
            connection = HBaseUtils.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            Result result = table.get(get);
            return buildDo(result);
        } catch (IOException e) {
            LOG.error("connection error when query", e);
            return null;
        } finally {
            closeTable(table);
        }
    }
    
    public List<T> query(List<String> rowkeys) {
        if (rowkeys.size() == 0) {
            return null;
        }
        
        Connection connection = null;
        Table table = null;
        try {
            connection = HBaseUtils.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            List<Get> gets = new ArrayList<Get>();
            for(String rowkey: rowkeys){
            	Get get = new Get(rowkey.getBytes());
            	gets.add(get);
            }
            Result[] results = table.get(gets);
            
            return buildDoList(results);
        } catch (IOException e) {
            LOG.error("connection error when query", e);
            return null;
        } finally {
            closeTable(table);
        }
    }


    /**
     * @param obj, not null & has row key
     * @return
     * @throws IllegalAccessException
     */
    private Put doPut(T obj) throws IllegalAccessException {
        Put put = new Put(obj.rowKeyBytes());
        Field[] fields = ReflectionUtil.getDeclaredFields(obj);
//        for (Field field : obj.getClass().getDeclaredFields()) {
        for (Field field : fields) {
             field.setAccessible(true);
        	 if(!columnAndFieldNullCheck(obj, field)){//不处理没有注解的
             	continue;
             }
        	
            if (field.isAnnotationPresent(JsonField.class)) {
                jsonBytesPut(put, obj, field);
            } else if (field.isAnnotationPresent(MapField.class)) {
                mapPut(put, obj, field);
            } else {
                pojoPut(put, obj, field);
            }
        }
        return put;
    }

    /**
     * @param Obj,   not null
     * @param field, null
     * @throws IllegalAccessException
     */
//    private boolean fieldNullCheck(T Obj, Field field) throws IllegalAccessException {
//        if (field.get(Obj) == null) {
//            //throw new IllegalStateException("object field " + field.getName() + " not initialized");
//        	return false;
//        }
//        return true;
//    }

    private boolean columnCheck(Field field) {
        if (!field.isAnnotationPresent(HBColumn.class)) {
//            throw new IncompleteAnnotationException(Column.class, "Column annotation required");
        	return false;//这里做判断，如果没有注解的，则不与hbase交互
        }
        String family = field.getAnnotation(HBColumn.class).family();
        String qualifier = field.getAnnotation(HBColumn.class).qualifier();
        if (family.isEmpty() || qualifier.isEmpty()) {
            throw new IncompleteAnnotationException(HBColumn.class, "Column annotation value is null");
//        	return false;
        }
        return true;
    }
    
    private boolean columnAndFieldNullCheck(T Obj, Field field){
    	if (!field.isAnnotationPresent(HBColumn.class)) {
    		return false;//这里做判断，如果没有注解的，则不与hbase交互
    	}
        String family = field.getAnnotation(HBColumn.class).family();
        String qualifier = field.getAnnotation(HBColumn.class).qualifier();
        if (family.isEmpty() || qualifier.isEmpty()) {
           throw new IncompleteAnnotationException(HBColumn.class, "Column annotation value is null");
        }
        try {
			if (field.get(Obj) == null) {//get(Object obj) ： 取得obj对象这个Field上的值，更好的办法是这里应该根据getter方法来判断
				return false;
			}
		} catch (Exception e) {
			LOG.error("connection error when query", e);
			return false;
		}
        return true;
    }

    private Object typeCast(Class<?> clz, byte[] bytes) {
        if (clz.equals(String.class)) {
            return Bytes.toString(bytes);
        } else if (clz.equals(Integer.class)) {
            return Bytes.toInt(bytes);
        } else if (clz.equals(Long.class)) {
            return Bytes.toLong(bytes);
        } else if (clz.equals(Boolean.class)) {
            return Bytes.toBoolean(bytes);
        } else if (clz.equals(Double.class)) {
            return Bytes.toDouble(bytes);
        } else if (clz.equals(Byte.class)) {
            return bytes[0];
        } else {
            throw new IllegalArgumentException("class type " + clz.getName() + " not support");
        }
    }

    private byte[] typeCast(Object obj) {
        if (obj instanceof Integer) {
            return Bytes.toBytes((Integer) obj);
        } else if (obj instanceof String) {
            return Bytes.toBytes((String) obj);
        } else if (obj instanceof Long) {
            return Bytes.toBytes((Long) obj);
        } else if (obj instanceof Boolean) {
            return Bytes.toBytes((Boolean) obj);
        } else if (obj instanceof Float) {
            return Bytes.toBytes((Float) obj);
        } else if (obj instanceof Byte) {
            return Bytes.toBytes((Byte) obj);
        } else if (obj instanceof Character) {
            return Bytes.toBytes((Character) obj);
        } else if (obj instanceof Double) {
            return Bytes.toBytes((Double) obj);
        } else {
        	LOG.error("typeCast field type " + obj.getClass().getName() + " not support");
            throw new IllegalArgumentException("field type " + obj.getClass().getName() + " not support");
        }
    }

    /**
     * fill Put with the Field of (T) obj
     *
     * @param put   [in/out]
     * @param obj
     * @param field
     * @return
     * @throws IllegalAccessException
     */
    private Put pojoPut(Put put, final T obj, final Field field) throws IllegalAccessException {
        String family = field.getAnnotation(HBColumn.class).family();
        String qualifier = field.getAnnotation(HBColumn.class).qualifier();
        Object fieldObj = field.get(obj);
        if(fieldObj==null){
        	return put;
        }
        /* 为了避免数据导入而不是代码插入数据的问题，这里存库全部转为String，取出时再次转换*/
        if (fieldObj instanceof Integer) {
//            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes(((Integer) fieldObj)));
        	put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes(Integer.toString((Integer) fieldObj)));
        } else if (fieldObj instanceof Boolean) {
            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes(((Boolean) fieldObj)));
            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes(Boolean.toString((Boolean) fieldObj)));
        } else if (fieldObj instanceof Long) {
            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes(Long.toString((Long) fieldObj)));
        } else if (fieldObj instanceof Float) {
            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes(Float.toString((Float) fieldObj)));
        } else if (fieldObj instanceof String) {
            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((String) fieldObj));
        } else if (fieldObj instanceof Byte) {
            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes(Byte.toString((Byte) fieldObj)));
        } else if (fieldObj instanceof Double) {
            put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes(Double.toString((Double) fieldObj)));
        } else if (fieldObj instanceof Date){
        	dateFormatPattern = field.getAnnotation(HBColumn.class).format();
        	if("".equals(dateFormatPattern)){
        		return put;
        	}
        	getDateFormat();
        	
        	put.addColumn(family.getBytes(), qualifier.getBytes(), Bytes.toBytes(dateFormat.format((Date) fieldObj)));
        } else {
            throw new IllegalArgumentException("field type " + fieldObj.getClass().getName() + " not support");
        }
        return put;
    }

    /**
     * fill Put with the Field of (T) obj;
     *
     * @param put   [in/out]
     * @param obj
     * @param field
     * @return
     * @throws IllegalAccessException
     */
    private Put jsonBytesPut(Put put, final T obj, final Field field) throws IllegalAccessException {
        String family = field.getAnnotation(HBColumn.class).family();
        String qualifier = field.getAnnotation(HBColumn.class).qualifier();
        put.addColumn(family.getBytes(), qualifier.getBytes(), JSON.toJSONBytes(field.get(obj)));
        return put;
    }

    /**
     * fill Put with the Field of (T) obj
     *
     * @param put   [in/out]
     * @param obj
     * @param field
     * @return
     * @throws IllegalAccessException
     */
    private Put mapPut(Put put, final T obj, final Field field) throws IllegalAccessException {

        String family = field.getAnnotation(HBColumn.class).family();
        Map<Object, Object> mp = (Map<Object, Object>) field.get(obj);
        for (Map.Entry<Object, Object> entry : mp.entrySet()) {
            put.addColumn(family.getBytes(), typeCast(entry.getKey()), typeCast(entry.getValue()));
        }
        return put;
    }

    private Put doUpdate(T obj) throws IllegalAccessException {
        Put put = new Put(obj.rowKeyBytes());
        Field[] fields = ReflectionUtil.getDeclaredFields(obj);
        for (Field field : fields) {
        	field.setAccessible(true);
            if(!columnAndFieldNullCheck(obj, field)){//不处理没有注解的
            	continue;
            }
            if (field.isAnnotationPresent(JsonField.class)) {
                jsonBytesPut(put, obj, field);
            } else {
                pojoPut(put, obj, field);
            }
        }
        return put;
    }
    
    private void closeTable(Table table) {
        try {
            if (table != null) {
                table.close();
            }
        } catch (IOException e) {
            LOG.error("close error", e);
        }
    }

//    private void closeAll(Table table, Connection connection) {
//        try {
//            if (table != null) {
//                table.close();
//            }
//            if (connection != null) {
//                connection.close();
//            }
//        } catch (IOException e) {
//            LOG.error("close error", e);
//        }
//    }

    private void parseJsonBytes(T stu, Field field, Result result) throws IllegalAccessException {
        HBColumn column = field.getAnnotation(HBColumn.class);
        byte[] val = result.getValue(column.family().getBytes(), column.qualifier().getBytes());
        field.set(stu, JSON.parseObject(new String(val), field.getType()));
    }

    private void parseMap(T stu, Field field, Result result) throws IllegalAccessException {
        HBColumn column = field.getAnnotation(HBColumn.class);
        MapField mapFieldAnnotation = field.getAnnotation(MapField.class);
        Class<?> keyClz = mapFieldAnnotation.keyClass();
        Class<?> valClz = mapFieldAnnotation.valueClass();
        NavigableMap<byte[], byte[]> mp = result.getFamilyMap(column.family().getBytes());
        Map<Object, Object> mapField = new HashMap<Object, Object>();
        for (Map.Entry<byte[], byte[]> entry : mp.entrySet()) {
            mapField.put(typeCast(keyClz, entry.getKey()), typeCast(valClz, entry.getValue()));
        }
        field.set(stu, mapField);
    }

    private void fillField(T obj, final Field field, final Result result) throws IllegalAccessException {
        HBColumn column = field.getAnnotation(HBColumn.class);
        if (field.isAnnotationPresent(JsonField.class)) {
            parseJsonBytes(obj, field, result);
        } else if (field.isAnnotationPresent(MapField.class)) {
            parseMap(obj, field, result);
        } else {
        	
        	 byte[] val = result.getValue(column.family().getBytes(), column.qualifier().getBytes());
        	 String value = Bytes.toString(val);
        	 if(val!=null && val.length>0){
        		 Class type = field.getType();
                 if (type.equals(Integer.class)) {
//                     field.set(obj, Bytes.toInt(val));//zl导入的数据全部会被转化为byte类型，对代码的转换有影响
                	 field.set(obj, Integer.parseInt(value));
                 } else if (type.equals(String.class)) {
//                     field.set(obj, Bytes.toString(val));
                	 field.set(obj, value);
                 } else if (type.equals(Long.class)) {
//                     field.set(obj, Bytes.toLong(val));
                	 field.set(obj, Long.parseLong(value));
                 } else if (type.equals(Boolean.class)) {
//                     field.set(obj, Bytes.toBoolean(val));
                	 field.set(obj, Boolean.parseBoolean(value));
                 } else if (type.equals(Double.class)) {
//                     field.set(obj, Bytes.toDouble(val));
                	 field.set(obj, Double.parseDouble(value));
                 } else if (type.equals(Float.class)) {
//                     field.set(obj, Bytes.toFloat(val));//zl导入的数据全部会被转化为byte类型，对代码的转换有影响
                	 field.set(obj, Float.parseFloat(value));
                 } else if (type.equals(Byte.class)) {
//                     field.set(obj, val[0]);
                	 field.set(obj, value.getBytes()[0]);
	        	 } else if (type.equals(Date.class)) {
	        		 dateFormatPattern = column.format();
                	 if("".equals(dateFormatPattern)){
                 		return ;
                 	 }
                 	 getDateFormat();
                	 try {
						field.set(obj, dateFormat.parse(new String(val)));
					} catch (Exception e) {
						e.printStackTrace();
					}
	             } else {
	            	 LOG.error("fillField field type " + obj.getClass().getName() + " not support");
                     throw new IllegalArgumentException("field type " + type.getName() + " is not support");
                 }
        	 }
        }
    }

    /**
     * @Title: buildDo  
     * @Description: 构建对象
     * 		注意：如果rowkey不是对象所有的，则要设值 
     * @param @param result
     * @param @return    参数  
     * @return T    返回类型  
     * @throws
     */
    private T buildDo(Result result) {
        T obj = null;
        if (result == null || result.isEmpty() || clz == null) {
            return obj;
        }
        try {
            obj = (T) clz.newInstance();
            Field[] fields = ReflectionUtil.getDeclaredFields(obj);
            for (Field field : fields) {
            	field.setAccessible(true);
            	if(!columnCheck(field)){//不处理没有注解的
            		continue;
                }
                fillField(obj, field, result);
            }
            if(!obj.hasRowKey()){
            	obj.setRowKey(new String(result.getRow()));
            }
            return obj;
        } catch (InstantiationException e) {
            LOG.error("new instance error when buildDo", e);
            return null;
        } catch (IllegalAccessException e) {
            LOG.error("some error", e);
            return null;
        }
    }
    
    /**
     * @Title: buildDoList  
     * @Description: 构建对象
     * 		注意：如果rowkey不是对象所有的，则要设值 
     * @param @param results
     * @param @return    参数  
     * @return List<T>    返回类型  
     * @throws
     */
    private List<T> buildDoList(Result[] results) {
        try {
            List<T> objs = new ArrayList<T>();
            List<Field> fields = ReflectionUtil.getHbaseDeclaredFields(clz);
            for(Result result: results){
            	T obj = (T) clz.newInstance();
            	for (Field field : fields) {
            		fillField(obj, field, result);
            	}
            	if(!obj.hasRowKey()){
                	obj.setRowKey(new String(result.getRow()));
                }
            	objs.add(obj);
            }
            
            return objs;
        } catch (InstantiationException e) {
            LOG.error("new instance error when buildDo", e);
            return null;
        } catch (IllegalAccessException e) {
            LOG.error("some error", e);
            return null;
        }
    }

    public DateFormat getDateFormat() {
        if (dateFormat == null) {
            if (dateFormatPattern != null) {
                dateFormat = new SimpleDateFormat(dateFormatPattern, defaultLocale);
                dateFormat.setTimeZone(defaultTimeZone);
            }
        }

        return dateFormat;
    }

}
