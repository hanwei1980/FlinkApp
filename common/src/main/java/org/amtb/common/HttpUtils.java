package org.amtb.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

@Slf4j
public class HttpUtils {

    /**
     * 封装GET请求
     *
     * @param url
     * @return
     * @throws IOException
     */
    public static String get(String url) {
        String result = "";
        // 接受请求返回的定义
        CloseableHttpResponse response = null;
        CloseableHttpClient httpClient = null;
        try {
            httpClient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet(url);
            // 定义连接超时时间
            RequestConfig requestConfig =
                    RequestConfig.custom().setSocketTimeout(Integer.parseInt(PropertyUtil.get("http.socket.timeout")))
                            .setConnectTimeout(Integer.parseInt(PropertyUtil.get("http.connect.timeout"))).build();
            httpGet.setConfig(requestConfig);
            // 执行get请求
            response = httpClient.execute(httpGet);
            if (response.getStatusLine().getStatusCode() == 200) {
                result = EntityUtils.toString(response.getEntity(), "utf-8");
            }
        } catch (Exception e) {
            log.error("org.amtb.common.HttpUtils-->get()::", e);
            return "org.amtb.common.HttpUtils.get() Error!";
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
                if (httpClient != null) {
                    httpClient.close();
                }
            } catch (Exception e) {
                log.error("org.amtb.common.HttpUtils-->response or httpClient Close Error!::", e);
                return "response or httpClient Close Error!";
            }
        }
        return result;
    }
}
