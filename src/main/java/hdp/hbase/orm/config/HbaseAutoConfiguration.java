package hdp.hbase.orm.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import hdp.hbase.orm.configure.HbaseConfig;
import hdp.hbase.orm.dao.HbaseTemplate;
/**  
 * @Title: HbaseAutoConfiguration.java
 * @Description: TODO(用一句话描述该文件做什么)
 * @author fengwei  
 * @date 2017年12月28日 上午11:24:17
 * @version V1.0  
 */
@org.springframework.context.annotation.Configuration
@EnableConfigurationProperties(HbaseConfig.class)
@ConditionalOnClass(HbaseTemplate.class)
public class HbaseAutoConfiguration {

    @Autowired
    private HbaseConfig hbaseConfig;
    
    @Bean(name="hbaseTemplate") 
    @ConditionalOnMissingBean(HbaseTemplate.class)
    public HbaseTemplate hbaseTemplate() {
        Configuration configuration = HBaseConfiguration.create();
//        configuration.addResource("hbase-site.xml");
        configuration.set(HConstants.ZOOKEEPER_QUORUM, hbaseConfig.getQuorum());
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, hbaseConfig.getPort());
        configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, hbaseConfig.getNodeParent());
        return new HbaseTemplate(configuration);
    }

}
