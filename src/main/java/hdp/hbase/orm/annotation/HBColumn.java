package hdp.hbase.orm.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 
 * @Title: HBColumn.java
 * @Description: TODO(用一句话描述该文件做什么)
 * @author fengwei  
 * @date 2017年7月17日 上午11:19:04
 * @version V1.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
public @interface HBColumn {
    String family();

    String qualifier();
    
    String format() default "";
}

