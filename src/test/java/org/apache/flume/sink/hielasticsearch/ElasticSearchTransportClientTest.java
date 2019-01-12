package org.apache.flume.sink.hielasticsearch;

import com.google.common.collect.Maps;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.sink.hielasticsearch.client.ElasticSearchTransportClient;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import static org.apache.flume.sink.hielasticsearch.ElasticSearchSinkConstants.DEFAULT_CLUSTER_NAME;

/**
 * Created by zhanghuan on 2019/1/8.
 */
public class ElasticSearchTransportClientTest {

    @Test
    public void send() throws Exception {

        TimeBasedIndexNameBuilder builder = new TimeBasedIndexNameBuilder();
        Context context = new Context();
        context.put(ElasticSearchSinkConstants.INDEX_NAME,"log_index");
        builder.configure(context);
        ElasticSearchEventSerializer serializer = new ElasticSearchDynamicSerializer();
        ElasticSearchTransportClient client = new ElasticSearchTransportClient(new String[]{"localhost:9300"}, DEFAULT_CLUSTER_NAME, serializer);
        client.addEvent(new Event() {
            public Map<String, String> getHeaders() {
                return Maps.newHashMap();
            }

            public void setHeaders(Map<String, String> map) {

            }

            public byte[] getBody() {
                try {
                    return "{\"ip\":\"192.168.1.103\",\"level\":\"WARN\",\"message\":\"[Thread-1] com.mine.log.Test test……\\n\",\"system\":\"mlog\",\"time\":\"2019-01-09T13:08:57.144Z\"}".getBytes("utf-8");
                } catch (UnsupportedEncodingException e) {
                    return null;
                }
            }

            public void setBody(byte[] bytes) {

            }
        }, builder,"log_table", 4l);
        client.execute();
    }

    @Test
    public void testEsTime(){
        DateTimeFormatter formatter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);
        System.err.println(formatter.print(new Date().getTime()));
    }

    @Test
    public void testUTCTime(){
        SimpleDateFormat utcFormater = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        utcFormater.setTimeZone(TimeZone.getTimeZone("UTC"));//时区定义并进行时间获取
        System.err.println(utcFormater.format(new Date()));
    }
}
