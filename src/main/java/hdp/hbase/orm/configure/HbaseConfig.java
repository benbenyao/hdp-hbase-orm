package hdp.hbase.orm.configure;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import lombok.Data;

/**  
 * @Title: HbaseConfig.java
 * @Description: 配置文件对象
 * @author fengwei  
 * @date 2017年12月26日 下午2:18:52
 * @version V1.0  
 */
@Component
@ConfigurationProperties(prefix = "hbase")
@Data
public class HbaseConfig {
	
    private String quorum;

    private String port;

    private String rootDir;

    private String nodeParent;

}
