package com.github.xuzw.forexroo_scheduler;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 徐泽威 xuzewei_2012@126.com
 * @time 2017年6月29日 上午10:28:12
 */
public class JvmUtils {
    public static String getIp() throws IOException {
        InputStream is = null;
        InputStreamReader reader = null;
        try {
            is = new URL("http://1212.ip138.com/ic.asp").openConnection().getInputStream();
            reader = new InputStreamReader(is, "GB2312");
            List<String> lines = IOUtils.readLines(reader);
            String content = StringUtils.join(lines, "");
            return content.substring(content.indexOf("[") + 1, content.indexOf("]"));
        } finally {
            IOUtils.closeQuietly(reader);
            IOUtils.closeQuietly(is);
        }
    }

    public static String getPid() {
        return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    }
}
