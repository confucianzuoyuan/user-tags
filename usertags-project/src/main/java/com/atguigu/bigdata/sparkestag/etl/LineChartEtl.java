package com.atguigu.bigdata.sparkestag.etl;

import com.atguigu.bigdata.sparkestag.support.SparkUtils;
import com.atguigu.bigdata.sparkestag.support.date.DateStyle;
import com.atguigu.bigdata.sparkestag.support.date.DateUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LineChartEtl {
    public static List<LineVo> lineVos(SparkSession session) {
        ZoneId defaultZoneId = ZoneId.systemDefault();
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);
        Date nowDay = Date.from(now.atStartOfDay(defaultZoneId).toInstant());
        Date sevenDayBefore = DateUtil.addDay(nowDay, -8);

        // regCount , memberCount (id auto increment)
        // 近七天注册人数统计
        String memberSql = "select date_format(create_time,'yyyy-MM-dd') as day," +
                " count(id) as regCount, max(id) as memberCount " +
                " from usertags.t_member where create_time >='%s' " +
                " group by date_format(create_time,'yyyy-MM-dd') order by day";
        memberSql = String.format(memberSql, DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> memberDs = session.sql(memberSql);

        // orderCount , gmv (t_order , id auto increment)
        // 近七天订单和流水统计
        String orderSql = "select date_format(create_time,'yyyy-MM-dd') as day," +
                " max(order_id) orderCount, sum(origin_price) as gmv" +
                " from usertags.t_order where create_time >='%s' " +
                "group by date_format(create_time,'yyyy-MM-dd') order by day";

        orderSql = String.format(orderSql, DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> orderDs = session.sql(orderSql);
        // left join a on a.id = b.id
        // 联接查询
        Dataset<Tuple2<Row, Row>> tuple2Dataset = memberDs.joinWith(orderDs, memberDs.col("day").equalTo(orderDs.col("day")), "inner");

        List<Tuple2<Row, Row>> tuple2s = tuple2Dataset.collectAsList();
        List<LineVo> vos = new ArrayList<>();
        for (Tuple2<Row, Row> tuple2 : tuple2s) {
            JSONObject obj = new JSONObject();
            Row row1 = tuple2._1();
            Row row2 = tuple2._2();
            StructType schema = row1.schema();
            String[] strings = schema.fieldNames();
            for (String string : strings) {
                Object as = row1.getAs(string);
                obj.put(string, as);
            }

            schema = row2.schema();
            strings = schema.fieldNames();
            for (String string : strings) {
                Object as = row2.getAs(string);
                obj.put(string, as);
            }

            LineVo lineVo = obj.toJavaObject(LineVo.class);
            vos.add(lineVo);
        }

        // 近七天之前的流水总和
        String gmvTotal = "select sum(origin_price) as totalGmv from usertags.t_order where create_time <'%s'";
        gmvTotal = String.format(gmvTotal, DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> gmvDs = session.sql(gmvTotal);
        double gmvAll = gmvDs.collectAsList().get(0).getDouble(0);
        BigDecimal decimalGmv = BigDecimal.valueOf(gmvAll);

        List<BigDecimal> destList = new ArrayList<>();
        for (int i = 0; i < vos.size(); i++) {
            LineVo lineVo = vos.get(i);
            BigDecimal gmv = lineVo.getGmv();
            BigDecimal temp = gmv.add(decimalGmv);

            for (int j = 0; j < i; j++) {
                LineVo prev = vos.get(j);
                temp = temp.add(prev.getGmv());
            }
            // ?
            destList.add(temp);

        }

        for (int i = 0; i < destList.size(); i++) {
            LineVo lineVo = vos.get(i);
            lineVo.setGmv(destList.get(i));
        }

        return vos;
    }

    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();
        List<LineVo> lineVos = lineVos(session);
        System.out.println(lineVos);
    }

    @Data
    static class LineVo {
        // 时间关联，线性增长的 mysql
        private String day;
        private Integer regCount;
        private Integer memberCount;
        private Integer orderCount;
        private BigDecimal gmv;
    }
}
