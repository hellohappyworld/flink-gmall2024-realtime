package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 创建者：gml
 * 创建日期：2024-05-09
 * 功能描述：交易域支付成功事务事实表
 * DWD_支付成功需求筛选支付成功数据 https://www.bilibili.com/video/BV1dv421y7eu?p=66&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_支付成功需求三张表格数据准备 https://www.bilibili.com/video/BV1dv421y7eu?p=67&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_支付成功需求使用interval join完成表格关联 https://www.bilibili.com/video/BV1dv421y7eu?p=68
 * DWD_支付成功需求使用lookupJoin完成维度退化 https://www.bilibili.com/video/BV1dv421y7eu?p=69&vd_source=b6440733352819cc788f24606ec23fa3
 * DWD_支付成功需求写出数据 https://www.bilibili.com/video/BV1dv421y7eu?p=70
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016, 4, "dwd_trade_order_pay_suc_detail");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //核心业务逻辑
        //1、读取topic_db数据
        createTopicDb(groupId, tableEnv);

        //2、筛选出支付成功的数据
        fliterPaymentTable(tableEnv);

        //3、读取下单详情表数据
        createDwdOrderDetail(tableEnv, groupId);

        //4、创建base_dic字典表
        createBaseDic(tableEnv);

        //5、使用 interval join 完成支付成功流和订单详情流的关联
        intervalJoin(tableEnv);

        //6、使用 lookup join 完成维度退化
        Table resultTable = lookupJoin(tableEnv);

        //7、创建 upsert kafka写出
        createUpsertKafkaSink(tableEnv);

        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();
    }

    public void createUpsertKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + "(\n" +
                "\tid STRING,\n" +
                "\torder_id STRING,\n" +
                "\tuser_id STRING,\n" +
                "\tpayment_type_code STRING,\n" +
                "\tpayment_type_name STRING,\n" +
                "\tpayment_time STRING,\n" +
                "\tsku_id STRING,\n" +
                "\tprovince_id STRING,\n" +
                "\tactivity_id STRING,\n" +
                "\tactivity_rule_id STRING,\n" +
                "\tcoupon_id STRING,\n" +
                "\tsku_name STRING,\n" +
                "\torder_price STRING,\n" +
                "\tsku_num STRING,\n" +
                "\tsplit_total_amount STRING,\n" +
                "\tsplit_activity_amount STRING,\n" +
                "\tsplit_coupon_amount STRING,\n" +
                "\tts bigint,\n" +
                " PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
    }

    public Table lookupJoin(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("SELECT\n" +
                "\tid,\n" +
                "\torder_id,\n" +
                "\tuser_id,\n" +
                "\tpayment_type payment_type_code,\n" +
                "\tinfo.dic_name payment_type_name,\n" +
                "\tpayment_time,\n" +
                "\tsku_id,\n" +
                "\tprovince_id,\n" +
                "\tactivity_id,\n" +
                "\tactivity_rule_id,\n" +
                "\tcoupon_id,\n" +
                "\tsku_name,\n" +
                "\torder_price,\n" +
                "\tsku_num,\n" +
                "\tsplit_total_amount,\n" +
                "\tsplit_activity_amount,\n" +
                "\tsplit_coupon_amount,\n" +
                "\tts\n" +
                "FROM pay_order p\n" +
                "left join base_dic FOR SYSTEM_TIME AS OF p.proc_time as b\n" +
                "on p.payment_type=b.rowkey");
    }

    public void intervalJoin(StreamTableEnvironment tableEnv) {
        Table payOrderTable = tableEnv.sqlQuery("SELECT\n" +
                "\tod.id,\n" +
                "\tp.order_id,\n" +
                "\tp.user_id,\n" +
                "\tpayment_type,\n" +
                "\tcallback_time payment_time,\n" +
                "\tsku_id,\n" +
                "\tprovince_id,\n" +
                "\tactivity_id,\n" +
                "\tactivity_rule_id,\n" +
                "\tcoupon_id,\n" +
                "\tsku_name,\n" +
                "\torder_price,\n" +
                "\tsku_num,\n" +
                "\tsplit_total_amount,\n" +
                "\tsplit_activity_amount,\n" +
                "\tsplit_coupon_amount,\n" +
                "\tp.ts,\n" +
                "\tp.proc_time\n" +
                "FROM payment p, order_detail od\n" +
                "WHERE p.order_id = od.order_id\n" +
                "AND p.row_time BETWEEN od.row_time - INTERVAL '15' MINUTE AND od.row_time + INTERVAL '15' SECOND\n");
        tableEnv.createTemporaryView("pay_order", payOrderTable);
    }

    public void createDwdOrderDetail(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table order_detail (\n" +
                " id STRING,\n" +
                " order_id STRING,\n" +
                " sku_id STRING,\n" +
                " sku_name STRING,\n" +
                " user_id STRING,\n" +
                " province_id STRING,\n" +
                " activity_id STRING,\n" +
                " activity_rule_id STRING,\n" +
                " coupon_id STRING,\n" +
                " order_price STRING,\n" +
                " sku_num STRING,\n" +
                " create_time STRING,\n" +
                " split_total_amount STRING,\n" +
                " split_activity_amount STRING,\n" +
                " split_coupon_amount STRING,\n" +
                " ts bigint,\n" +
                " row_time as TO_TIMESTAMP_LTZ(ts*1000,3), \n" +
                " WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND \n" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, groupId));
    }

    public void fliterPaymentTable(StreamTableEnvironment tableEnv) {
        Table paymentTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['order_id'] order_id,\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['payment_type'] payment_type,\n" +
                "\t`data`['total_amount'] total_amount,\n" +
                "\t`data`['callback_time'] callback_time,\n" +
                "\tts,\n" +
                "\trow_time,\n" +
                "\tproc_time\n" +
                "from topic_db \n" +
                "where `database`='gmall' and \n" +
                "`table`='payment_info' and\n" +
                "`type`='update' and \n" +
                "`old`['payment_status'] is not null and\n" +
                "`data`['payment status']='1602'");
        tableEnv.createTemporaryView("payment", paymentTable);
    }
}
