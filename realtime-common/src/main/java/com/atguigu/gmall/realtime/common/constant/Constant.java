package com.atguigu.gmall.realtime.common.constant;

/**
 * 创建者：gml
 * 创建日期：2024-04-01
 * 功能描述：常量类
 * 96_DWS_关键词开窗统计数据写出到doris  https://www.bilibili.com/video/BV1dv421y7eu?p=96&vd_source=b6440733352819cc788f24606ec23fa3
 */
public class Constant {
    public static final String KAFKA_BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";

    public static final String MYSQL_HOST = "hadoop102";
    public static final int MYSOL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "000000";

    public static final String PROCESS_DATABASE = "gmall2023_config";
    public static final String PROCESS_DIM_TABLE_NAME = "table_process_dim";
    public static final String PROCESS_DWD_TABLE_NAME = "table_process_dwd";

    public static final String HBASE_NAMESPACE = "gmall";
    public static final String HBASE_Z00KEEPER_QUORUM = "hadoop102,hadoop103,hadoop104";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSOL_URL = "jdbc:mysql://hadoop102:3306?usessL=false";

    public static final String FENODES = "hadoop102:7030";
    public static final String DORIS_DATABASE = "gmall2023_realtime";
    public static final String DORIS_USERNAME = "root";
    public static final String DORIS_PASSWORD = "aaaaaa";
    public static final String DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW = "dws_traffic_source_keyword_page_view_window";
    public static final String DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW = "dws_traffic_vc_ch_ar_is_new_page_view_window";
    public static final String DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW = "dws_traffic_home_detail_page_view_window";
    public static final String DWS_USER_USER_LOGIN_WINDOW = "dws_user_user_login_window";
    public static final String DWS_USER_USER_REGISTER_WINSOW = "dws_user_user_register_winsow";
    public static final String DWS_TRADE_CART_ADD_UU_WINDOW = "dws_trade_cart_add_uu_window";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd traffic start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd traffic page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd traffic action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd traffic display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INF0 = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd trade order cancel";
    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd trade_order_refund";
    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd trade_refund payment success";
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
}
