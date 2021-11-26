package com.youshu;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.phoenix.queryserver.client.ThinClientUtil;


import java.security.PublicKey;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

public class PhoenixSinkTest extends RichSinkFunction<String> {
    Connection connection,mysqlconn;
    PreparedStatement dStmt_u,dStmt_a,dStmt_s,dStmt_m,dStmt_c,dStmt_e,uStmt_u,uStmt_a,uStmt_s,uStmt_m,uStmt_c,uStmt_e,sumStmt_s,getClStmt;
    private Connection getConnection(String dbtype) {
        Connection conn = null;
        try {
            if(dbtype.equals("hbase")) {
                    Class.forName("org.apache.phoenix.queryserver.client.Driver");
                    String url = "jdbc:phoenix:thin:url=http://10.0.0.107:8765;serialization=PROTOBUF;serverTimezone=GMT%2B8";
            Properties pro = new Properties();
//            pro.setProperty("phoenix.insert.batchSize", "3");
//            pro.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, "15000000"); //改变默认的500000
//            pro.setProperty(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, "200000");
//            pro.setProperty(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB, "1048576000");
//            pro.setProperty("phoenix.index.failure.block.write","true");
//            pro.setProperty("phoenix.insert.batchSize", "50000");
            conn = DriverManager.getConnection(url,pro);
//
//                String test01 = ThinClientUtil.getConnectionUrl("10.0.0.107", 8765);
                // 2. 获取连接
//                conn = DriverManager.getConnection(test01, pro);
            } else if (dbtype.equals("mysql")){
                conn = DriverManager.getConnection("jdbc:mysql://114.55.73.174:3306/novel?tinyInt1isBit=false&useSSL=false&serverTimezone=Asia/Shanghai","super","hMh90XJYdy4KfxM7QR8V");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }





    public void open(Configuration parameters) throws Exception {
        connection = getConnection("hbase");

//        String updateSql_user = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,SEX,OPEN_ID,CHANNEL_ID,POP_ID,NICK_NAME,HEAD_IMG,PROXY_ID,BALANCE,IS_RECHARGE,IS_ATTENTION,RGT,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        String updateSql_user = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,SEX,OPEN_ID,CHANNEL_ID,BALANCE,IS_RECHARGE,IS_ATTENTION,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)";
        String updateSql_att  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,ATTENTION_AT) VALUES (?,?)";
        String updateSql_sec  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,RBC) VALUES (?,?)";
        String updateSql_mob  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,MOBILE) VALUES (?,?)";
        String updateSql_cop  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,CPSSTATUS,CPSEXPIRE) VALUES (?,?,?)";
        String updateSql_exi  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,VSD,VED) VALUES (?,?,?)";

//      String deleteSql_user = "DELETE FROM YOUSHUGE.NA_USER_BASE_LABLE WHERE USER_ID=?";
//        String deleteSql_user = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,SEX,OPEN_ID,CHANNEL_ID,POP_ID,NICK_NAME,HEAD_IMG,PROXY_ID,BALANCE,IS_RECHARGE,IS_ATTENTION,RGT,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
//        String deleteSql_att  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,ATTENTION_AT) VALUES (?,?)";
//        String deleteSql_sec  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,RBCA,RBC) VALUES (?,?,?)";
//        String deleteSql_mob  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,MOBILE) VALUES (?,?)";
//        String deleteSql_cop  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,CPS) VALUES (?,?,?)";
//        String deleteSql_exi  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,VIP) VALUES (?,?,?)";


        String deleteSql_user = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,SEX,OPEN_ID,CHANNEL_ID,BALANCE,IS_RECHARGE,IS_ATTENTION,RGT,created_at,updated_at) VALUES (?,null,null,null,null,null,null,null,null,null )";
        String deleteSql_att  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,ATTENTION_AT) VALUES (?,null)";
        String deleteSql_sec  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,RBC) VALUES (?,null)";
        String deleteSql_mob  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,MOBILE) VALUES (?,null)";
        String deleteSql_cop  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,CPSSTATUS,CPSEXPIRE) VALUES (?,null,null)";
        String deleteSql_exi  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,VSD,VED) VALUES (?,null,null)";

//        String deleteSql_user = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,SEX,OPEN_ID,CHANNEL_ID,BALANCE,IS_RECHARGE,IS_ATTENTION,created_at,updated_at) VALUES (?,'','','','','','','','')";
//        String deleteSql_att  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,ATTENTION_AT) VALUES (?,'')";
//        String deleteSql_sec  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,RBC) VALUES (?,'')";
//        String deleteSql_mob  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,MOBILE) VALUES (?,'')";
//        String deleteSql_cop  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,CPSSTATUS,CPSEXPIRE) VALUES (?,'','')";
//        String deleteSql_exi  = "UPSERT INTO YOUSHUGE.NA_USER_BASE_LABLE(USER_ID,VSD,VED) VALUES (?,'','')";

//        String sumSql_sec = "select sum(securities)  from ?.? where user_id= ? and expire > now()";


        uStmt_s = connection.prepareStatement(updateSql_sec);
        uStmt_u = connection.prepareStatement(updateSql_user);
        uStmt_a = connection.prepareStatement(updateSql_att);
        uStmt_m = connection.prepareStatement(updateSql_mob);
        uStmt_c = connection.prepareStatement(updateSql_cop);
        uStmt_e = connection.prepareStatement(updateSql_exi);

        dStmt_s = connection.prepareStatement(deleteSql_sec);
        dStmt_u = connection.prepareStatement(deleteSql_user);
        dStmt_a = connection.prepareStatement(deleteSql_att);
        dStmt_m = connection.prepareStatement(deleteSql_mob);
        dStmt_c = connection.prepareStatement(deleteSql_cop);
        dStmt_e = connection.prepareStatement(deleteSql_exi);

//        sumStmt_s = mysqlconn.prepareStatement(sumSql_sec);

    }

    // 每条记录插入时调用一次
    public void invoke(String value, Context context) throws Exception {

        String database = (String) JSONObject.parseObject(value).get("database");
        String table = (String) JSONObject.parseObject(value).get("table");
        String type = (String) JSONObject.parseObject(value).get("type");
        JSONObject data = (JSONObject) JSONObject.parseObject(value).get("data");

        switch (database){
            case "novel":
                System.out.println("novel");
                switch (table){
                    case "na_user" :
                        System.out.println("na_user");
                        opNaUser(type,data);
                        break;
                    case "na_user_attention" :
                        System.out.println("na_user_attention");
//                        System.out.println("----------------->"+data);
                        opNaUserAttention(type,data);
                        break;
                    case "na_user_mobile" :
                        System.out.println("na_user_mobile");
                        opNaUserMobile(type,data);
                        break;
                    case "na_user_coupons" :
                        System.out.println("na_user_coupons");
                        opNaUserCoupons(type,data);
                        break;
                    case "na_user_exif" :
                        System.out.println("na_user_exif");
                        opNaUserExif(type,data);
                        break;
                }
                break;
            case "user_securities":
                System.out.println("user_securities");
                opNaUserSecurities(type,data,table,database);
                break;
        }

    }


    public void close() throws Exception {

        if(uStmt_s != null) {
            uStmt_s.close();
        }
        if(uStmt_u != null) {
            uStmt_u.close();
        }
        if(uStmt_a != null) {
            uStmt_a.close();
        }
        if(uStmt_e != null) {
            uStmt_e.close();
        }
        if(uStmt_m != null) {
            uStmt_m.close();
        }
        if(uStmt_c != null) {
            uStmt_c.close();
        }
        if(dStmt_s != null) {
            dStmt_s.close();
        }
        if(dStmt_u != null) {
            dStmt_u.close();
        }
        if(dStmt_a != null) {
            dStmt_a.close();
        }
        if(dStmt_e != null) {
            dStmt_e.close();
        }
        if(dStmt_m != null) {
            dStmt_m.close();
        }
        if(dStmt_c != null) {
            dStmt_c.close();
        }

        if(connection != null) {
            connection.close();
        }
        if(mysqlconn != null) {
            mysqlconn.close();
        }

        if(getClStmt != null){
            getClStmt.close();
        }
    }

    public void opNaUser(String type,JSONObject data) {


        try {
            if ("create".equals(type) || "update".equals(type) ) {
                System.out.println("insert => " + data);
                String id =  String.valueOf(data.get("id")) ;

                Integer sex = data.getInteger("sex") ;
                String open_id = data.getString("open_id");
                Integer channel_id =  data.getInteger("channel_id");
    //            Object pop_id = data.get("pop_id");
    //            Object nick_name = data.get("nick_name");
    //            Object head_img =  data.get("head_img");
    //            Object proxy_id = data.get("proxy_id");
                Integer balance = data.getInteger("balance");
                Integer is_recharge =  data.getInteger("is_recharge");
                Integer is_attention = data.getInteger("is_attention");

                Object created_at = getCSTTime(data.get("created_at")) ;
                Object updated_at = getCSTTime(data.get("updated_at"));

    //            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //            String dt = sdf.format((java.sql.Timestamp)created_at);

    //            uStmt_u.setString(1, id);
    //            uStmt_u.setString(2, sex);
    //            uStmt_u.setObject(3,open_id);
    //            uStmt_u.setObject(4, channel_id);
    //            uStmt_u.setObject(5,pop_id );
    //            uStmt_u.setObject(6, nick_name);
    //            uStmt_u.setObject(7,head_img);
    //            uStmt_u.setObject(8,proxy_id );
    //            uStmt_u.setObject(9, balance);
    //            uStmt_u.setObject(10,is_recharge);
    //            uStmt_u.setObject(11,is_attention);
    //            uStmt_u.setObject(12,getDay(dt).intValue());
    //            uStmt_u.setObject(13,created_at);
    //            uStmt_u.setObject(14,updated_at);
    //            uStmt_u.executeUpdate();
                isAliveStmt(uStmt_u);

                uStmt_u.setString(1,id);
                uStmt_u.setInt(2,sex);
                uStmt_u.setString(3,open_id);
                uStmt_u.setInt(4,channel_id);
                uStmt_u.setInt(5,balance);
                uStmt_u.setInt(6,is_recharge);
                uStmt_u.setInt(7,is_attention);
    //            uStmt_u.setObject(8,getDay(dt).intValue());
                uStmt_u.setObject(8,created_at);
                uStmt_u.setObject(9,updated_at);
                uStmt_u.executeUpdate();
                connection.commit();
            }else if ("delete".equals(type)) {
                System.out.println("delete => " + data);

                    String id =  String.valueOf(data.get("id")) ;

                    isAliveStmt(dStmt_u);

                    dStmt_u.setString(1,id);
                    dStmt_u.executeUpdate();
                    connection.commit();
            }
        } catch (Exception e) {
            System.out.println("*********"+data+"*********"+"写入失败");
            e.printStackTrace();


        }
    }

    public void opNaUserAttention(String type,JSONObject data)  {
        try {
            if ("create".equals(type) || "update".equals(type) ) {
                System.out.println("insert => " + data);

                String user_id =  String.valueOf(data.get("user_id")) ;
                Object updated_at = getCSTTime(data.get("updated_at"));

                isAliveStmt(uStmt_a);

                uStmt_a.setString(1,user_id);
                uStmt_a.setObject(2,updated_at);
                uStmt_a.executeUpdate();
                connection.commit();
            }else if ("delete".equals(type)) {
                System.out.println("delete => " + data);
                String user_id =  String.valueOf(data.get("user_id")) ;

                isAliveStmt(dStmt_a);

                dStmt_a.setString(1,user_id);
    //            dStmt_a.setString(2, null);
                dStmt_a.executeUpdate();
                connection.commit();
            }
        } catch (Exception e) {
            System.out.println("*********"+data+"*********"+"写入失败");
            e.printStackTrace();
        }
    }

    public void opNaUserMobile(String type,JSONObject data) throws Exception{
        try {
            if ("create".equals(type) || "update".equals(type)) {
                System.out.println("insert => " + data);

                String user_id =  String.valueOf(data.get("user_id")) ;
                String mobile = data.getString("mobile");

                isAliveStmt(uStmt_m);

                uStmt_m.setString(1, user_id);
                uStmt_m.setString(2, mobile);
                uStmt_m.executeUpdate();
                connection.commit();

            }else if ("delete".equals(type)) {
                System.out.println("delete => " + data);
                String user_id =  String.valueOf(data.get("user_id")) ;

                isAliveStmt(dStmt_m);

                dStmt_m.setString(1, user_id);
    //            dStmt_m.setString(2, null);
                dStmt_m.executeUpdate();
                connection.commit();
            }
        } catch (Exception e) {
            System.out.println("*********"+data+"*********"+"写入失败");
            e.printStackTrace();
        }
    }

    public void opNaUserCoupons(String type,JSONObject data) throws Exception{
        try {
            if ("create".equals(type) || "update".equals(type) || "read".equals(type) ) {
                System.out.println("insert => " + data);
    //            int cps = -1;
                String user_id =  String.valueOf(data.get("user_id")) ;
                Integer status = data.getInteger("status");
                Object expire = data.get("expire");

    //            if (status == 1 || System.currentTimeMillis() / 1000 > expire){
    //                cps = 0;
    //            }else {
    //                cps = 1;
    //            }

                isAliveStmt(uStmt_c);

                uStmt_c.setString(1, user_id);
                uStmt_c.setInt(2, status);
                uStmt_c.setObject(3, expire);

                uStmt_c.executeUpdate();
                connection.commit();
            }else if ("delete".equals(type)) {
                System.out.println("delete => " + data);
                String user_id =  String.valueOf(data.get("user_id")) ;

                isAliveStmt(dStmt_c);

                dStmt_c.setString(1, user_id);
    //            dStmt_c.setObject(2, null);
    //            dStmt_c.setObject(3,null);
                dStmt_c.executeUpdate();
                connection.commit();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void opNaUserExif(String type,JSONObject data) throws Exception{
        try {
            if ("create".equals(type) || "update".equals(type) || "read".equals(type) ) {
                System.out.println("insert => " + data);
                String user_id =  String.valueOf(data.get("user_id")) ;
                Object started_date = getCSTTime(data.get("started_date"));
                Object deadline_date = getCSTTime(data.get("deadline_date"));

                isAliveStmt(uStmt_e);

                uStmt_e.setString(1, user_id);
                uStmt_e.setObject(2, started_date);
                uStmt_e.setObject(3, deadline_date);
                uStmt_e.executeUpdate();
                connection.commit();
            }else if ("delete".equals(type)) {
                System.out.println("delete => " + data);
                String user_id =  String.valueOf(data.get("user_id")) ;

                isAliveStmt(dStmt_e);

                dStmt_e.setString(1, user_id);
    //            dStmt_e.setObject(2, null);
    //            dStmt_e.setObject(3, null);
                dStmt_e.executeUpdate();
                connection.commit();
            }
        } catch (Exception e) {
            System.out.println("*********"+data+"*********"+"写入失败");
            e.printStackTrace();
        }
    }

    public void opNaUserSecurities(String type,JSONObject data,String table,String database) throws Exception{
        try {
            if ("create".equals(type) || "update".equals(type) ) {
                System.out.println("insert => " + data);

                mysqlconn = getConnection("mysql");


                String user_id =  String.valueOf(data.get("user_id")) ;
    //            Object rbca = data.get("securities_all");

                String sumSql_sec = "select sum(securities)  from " + database+ "." + table +  " where user_id=" +user_id+ " and expire > unix_timestamp(now())";

    //            System.out.println("======================>"+sumSql_sec);
                sumStmt_s = mysqlconn.prepareStatement(sumSql_sec);

    //            sumStmt_s.setObject(1,database);
    //            sumStmt_s.setObject(2,table);
    //            sumStmt_s.setObject(3,user_id);
                ResultSet rs = sumStmt_s.executeQuery();

                int rbc = -1;
                if(rs.next()) {
                    rbc = rs.getInt(1);
                    System.out.println("======================>"+rbc);
                }

                isAliveStmt(uStmt_s);

                uStmt_s.setString(1, user_id);
    //            uStmt_s.setObject(2, rbca);
                uStmt_s.setInt(2, rbc);

                uStmt_s.executeUpdate();
                connection.commit();
            }else if ("delete".equals(type)) {
                System.out.println("Nodelete => " + data);
                String user_id =  String.valueOf(data.get("user_id")) ;

                isAliveStmt(dStmt_s);

                dStmt_s.setString(1, user_id);
    //            dStmt_s.setObject(2, null);
    //            dStmt_s.setObject(3, null);
    //            dStmt_s.executeUpdate();
                connection.commit();
            }
        } catch (Exception e) {
            System.out.println("*********"+data+"*********"+"写入失败");
            e.printStackTrace();
        }
    }


    /*
        判读时间差距，两个时间相差多少天，时，分，秒
         */
//    public static Long getDay(String date) {
//        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        Long days = null;
//        try {
//            Date currentTime = dateFormat.parse(dateFormat.format(new Date()));//现在系统当前时间
//            Date pastTime = dateFormat.parse(date);//过去时间
//            long diff = currentTime.getTime() - pastTime.getTime();
//            days = diff / (1000 * 60 * 60 * 24);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        return days;
//    }


    public Object getCSTTime(Object ts) throws Exception{
        try {
            DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            Date dt = sdf.parse(ts.toString());
            Calendar gc = Calendar.getInstance();
            gc.setTime(dt);
            gc.add(Calendar.HOUR_OF_DAY, 8 );
            return sdf.format(gc.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    public int getChannelId(int user_id,String database,String table) throws Exception {


        try {
            mysqlconn = getConnection("mysql");
            String sql = "select channel_id from novel.na_user where id=" + user_id;
            getClStmt  = mysqlconn.prepareStatement(sql);
            ResultSet rs = getClStmt.executeQuery();

            int channel_id = 0;
            if(rs.next()) {
                channel_id = rs.getInt(1);
            }

            return channel_id;
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public void isAliveStmt(PreparedStatement pstmt) throws Exception {
        try {
            if (pstmt == null){
                System.out.println("phoenix连接丢失,重新创建......");
                close();
                open(new Configuration());
            } else {
                System.out.println("phoenix连接存在......");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

