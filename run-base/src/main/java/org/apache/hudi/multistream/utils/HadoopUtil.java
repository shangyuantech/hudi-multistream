package org.apache.hudi.multistream.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;

public class HadoopUtil {

    private final static Logger log = LoggerFactory.getLogger(HadoopUtil.class);

    public static void initKrb(String krb5conf, String krbLogin, String keytab) {
        log.info("init hadoop kerberos");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("java.security.krb5.conf", krb5conf);

        try {
            UserGroupInformation.setConfiguration(new Configuration());
            UserGroupInformation.loginUserFromKeytab(SecurityUtil.getServerPrincipal(krbLogin,
                    InetAddress.getLocalHost().getHostName()), keytab);
            log.info("login kerberos with user {}", UserGroupInformation.getCurrentUser());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
