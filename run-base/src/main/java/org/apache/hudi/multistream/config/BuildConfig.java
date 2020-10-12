package org.apache.hudi.multistream.config;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.multistream.utils.HadoopUtil;
import org.apache.hudi.multistream.utils.HudiUtil;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.SchedulerConfGenerator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Map;

@Configuration
public class BuildConfig {

    public static String ipAddress;

    @Value("${spring.cloud.client.ip-address}")
    public void setIpAddress(String ipAddress) {
        BuildConfig.ipAddress = ipAddress;
    }

    @Autowired
    private KafkaAdmin admin;

    @Bean
    public AdminClient adminClient() {
        AdminClient client = AdminClient.create(admin.getConfig());
        return client;
    }

    @Value("${spring.cloud.consul.discovery.service-name}")
    private String serviceName;

    public static Integer serverPort;

    @Value("${server.port}")
    public void setServerPort(Integer serverPort) {
        BuildConfig.serverPort = serverPort;
    }

    @Autowired
    private HudiConfig hudiConfig;

    @Value("${hudi.hadoop.kerberos.conf:/etc/krb5.conf}")
    public String krb5conf;

    @Value("${hudi.hadoop.kerberos.login:null}")
    public String login;

    @Value("${hudi.hadoop.kerberos.keytab:null}")
    public String keytab;

    @Value("${hudi.hadoop.kerberos.enable:false}")
    public Boolean kerberosEnable;

    @Bean
    public JavaSparkContext getSparkContext() throws Exception {
        if (kerberosEnable) {
            HadoopUtil.initKrb(krb5conf, login, keytab);
        }

        Map<String, String> additionalSparkConfigs = SchedulerConfGenerator.getSparkSchedulingConfigs(hudiConfig.getDeltaConfig());
        return UtilHelpers.buildSparkContext(String.format("delta-streamer-%s-%s-%s", serviceName, HudiUtil.getInnetIp(), serverPort),
                hudiConfig.getDeltaConfig().sparkMaster, additionalSparkConfigs);
    }
}
