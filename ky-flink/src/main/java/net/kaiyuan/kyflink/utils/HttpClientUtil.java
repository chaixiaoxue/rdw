package net.kaiyuan.kyflink.utils;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

/*
 * 利用HttpClient进行post请求的工具类
 */
public class HttpClientUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientUtil.class);
    private static final String CHARSET_UTF_8 = "UTF-8";

    public static String doPost(String url, Map<String, Object> map) {
        return doPost(url, map, CHARSET_UTF_8);
    }

    public static void main(String[] args) {
        List<Map<String,Object>> list = new ArrayList<>();
        Map<String,Object> parms = new HashMap();
        Map<String,Object> parms1 = new HashMap();
        parms.put("id","b7f31a9f-4d6b-4010-953c-ffa1d1516ab2");
        parms1.put("tableName","qydp_mt_loan");
        parms.put("data",parms1);
        list.add(parms);
        doPost("http://test-hdp-03:9097/send",list,"utf-8");
    }

    public static String doPost(String url, Map<String, Object> map, String charset) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(url);
        String result = null;
        try {
            //httpClient = HttpClients.createDefault();
            //httpPost = new HttpPost(url);
            // 设置参数
            String json = JSON.toJSONString(map);
            StringEntity entity = new StringEntity(json, "UTF-8");
            httpPost.setHeader("Content-Type", "application/json; charset=UTF-8");
            httpPost.setEntity(entity);
            CloseableHttpResponse response = httpClient.execute(httpPost);
            LOGGER.info("Execute http post method,the url is ===={} , entity is {}", httpPost.getURI(), map);
            if (response != null) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    result = EntityUtils.toString(resEntity, charset);
                }
            }
            LOGGER.info("The http post url is {}, and result is {}", httpPost.getURI(), result);
        } catch (Exception ex) {
            LOGGER.error("Http post method error,the error is {}", ex);
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }


    public static String doPost(String url, List<Map<String,Object>> map, String charset) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(url);
        String result = null;
        try {
            //httpClient = HttpClients.createDefault();
            //httpPost = new HttpPost(url);
            // 设置参数
            String json = JSON.toJSONString(map);
            StringEntity entity = new StringEntity(json, "UTF-8");
            httpPost.setHeader("Content-Type", "application/json; charset=UTF-8");
            httpPost.setEntity(entity);
            CloseableHttpResponse response = httpClient.execute(httpPost);
            LOGGER.info("Execute http post method,the url is ===={} , entity is {}", httpPost.getURI(), map);
            if (response != null) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    result = EntityUtils.toString(resEntity, charset);
                }
            }
            LOGGER.info("The http post url is {}, and result is {}", httpPost.getURI(), result);
        } catch (Exception ex) {
            LOGGER.error("Http post method error,the error is {}", ex);
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public static String doPost(String url, Map<String, Object> map, Map<String, Object> headers, String charset) {
        CloseableHttpClient httpClient = null;
        HttpPost httpPost = null;
        String result = null;
        try {
            httpClient = HttpClients.createDefault();
            httpPost = new HttpPost(url);
            // 设置参数
            StringEntity entity = new StringEntity(JSON.toJSONString(map), "UTF-8");
            if (null != headers && headers.size() > 0) {
                Set<String> headerNames = headers.keySet();
                for (String name : headerNames) {
                    httpPost.addHeader(name, (String) headers.get(name));
                }
            }
            httpPost.setEntity(entity);
            CloseableHttpResponse response = httpClient.execute(httpPost);
            LOGGER.info("Execute http post method,the url is ===={} , entity is {}", httpPost.getURI(),
                    JSON.toJSONString(map));
            if (response != null) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    result = EntityUtils.toString(resEntity, charset);
                }
            }
            LOGGER.info("The http post url is {}, and result is {}", httpPost.getURI(), result);
        } catch (Exception ex) {
            LOGGER.error("Http post method error,the error is {}", ex);
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public static String doGet(String url, Map<String, Object> map, Map<String, String> headers, String charset) {
        String result = null;
        CloseableHttpClient httpClient = null;
        if (StringUtils.isEmpty(charset)) {
            charset = CHARSET_UTF_8;
        }
        try {
            httpClient = HttpClients.createDefault();
            URI uri = null;
            URIBuilder uriBuilder = new URIBuilder(url);
            // 准备参数
            if (null != map && map.size() > 0) {
                Iterator<Entry<String, Object>> iterator = map.entrySet().iterator();
                while (iterator.hasNext()) {
                    Entry<String, Object> entry = iterator.next();
                    uriBuilder.addParameter(entry.getKey(), entry.getValue().toString());
                }
            }
            uri = uriBuilder.build();
            HttpGet httpGet = new HttpGet(uri);
            if (headers != null) {
                Set<Entry<String, String>> entrySet = headers.entrySet();
                Iterator<Entry<String, String>> iterator = entrySet.iterator();
                while (iterator.hasNext()) {
                    Entry<String, String> next = iterator.next();
                    httpGet.setHeader(next.getKey(), next.getValue());
                }
            }
            CloseableHttpResponse response = httpClient.execute(httpGet);
            // LOGGER.info("Execute Http Get method,the url is ====" + httpGet.getURI());
            if (response != null) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    result = EntityUtils.toString(resEntity, charset);
                }
            }
            // LOGGER.info("The http get url is {}, and result is {}", httpGet.getURI(), result);
        } catch (Exception e) {
//            LOGGER.error("Http get method error,the error is {}", e);
            System.out.println(e);
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                LOGGER.error("Close http client error,the error is {}", e);
            }
        }
        return result;
    }

    public static String doDelete(String url) throws IOException {
        CloseableHttpClient httpClient = null;
        HttpDelete httpDelete = null;
        String result = null;
        try {
            httpClient = HttpClients.createDefault();
            httpDelete = new HttpDelete(url);
            // 设置参数
            CloseableHttpResponse response = httpClient.execute(httpDelete);
            LOGGER.info("Execute http post method,the url is ===={}", httpDelete.getURI());
            if (response != null) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    result = EntityUtils.toString(resEntity, CHARSET_UTF_8);
                }
            }
            LOGGER.info("The http delete url is {}, and result is {}", httpDelete.getURI(), result);
        } catch (Exception ex) {
            LOGGER.error("Http delete method error,the error is {}", ex);
        } finally {
            httpClient.close();
        }
        return result;
    }

    public static String doGet(String url, Map<String, Object> map, String charset) {
        return doGet(url, map, null, charset);
    }

    public static String doGet(String url) {
        return doGet(url, null, null, CHARSET_UTF_8);
    }

    public static String doGet(String url, Map<String, Object> map) {
        return doGet(url, map, null, CHARSET_UTF_8);
    }



}