package org.amtb.common;

import java.util.ResourceBundle;

/**
 * 配置文件工具类
 */
public class PropertyUtil {

    static ResourceBundle resourceBundle;

    static {
        resourceBundle = ResourceBundle.getBundle("config");
    }

    public static String get(String key) {
        return resourceBundle.getString(key);
    }

}
