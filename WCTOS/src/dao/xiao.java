package dao;

import domain.AccountInfo;
import domain.UserInfo;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

public class xiao {
    public static String login(String USERID, String password) throws IOException {
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);

        Table table = con.getTable(TableName.valueOf("user"));
        Get get = new Get(USERID.getBytes());
        get.addColumn("accountinfo".getBytes(), "password".getBytes());
        get.addColumn("accountinfo".getBytes(), "frozen".getBytes());

        Result result = table.get(get);
        String re_forzen = Bytes.toString(result.getValue("accountinfo".getBytes(), "frozen".getBytes()));

        table.close();
        con.close();

        //已被冻结
        if (re_forzen.equals("true")) {
            return "Frozen";
        }
        String re_password = Bytes.toString(result.getValue("accountinfo".getBytes(), "password".getBytes()));
        //密码吻合
        if (re_password.equals(password)) {
            return "OK";
        }
        //密码不吻合
        return "Fail";
    }

    public static String regist(String username, String password) throws IOException {
        String userID = RandomStringUtils.randomAlphanumeric(5) + System.currentTimeMillis();
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        Table table = con.getTable(TableName.valueOf("user"));
        Put put = new Put(userID.getBytes());
        put.addColumn("userinfo".getBytes(), "username".getBytes(), username.getBytes());
        put.addColumn("accountinfo".getBytes(), "password".getBytes(), password.getBytes());
        table.put(put);

        table.close();
        con.close();
        return userID;
    }

    public static AccountInfo getInfo(String USERID) throws IOException {
        AccountInfo accountInfo = new AccountInfo();
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库管理对象
        Admin conAdmin = con.getAdmin();

        //获取数据库表
        Table table = con.getTable(TableName.valueOf("user"));

        //配置Get对象，定义需要返回的结构
        Get get = new Get(USERID.getBytes());
        get.addColumn("accountinfo".getBytes(), "password".getBytes());
        get.addColumn("accountinfo".getBytes(), "tel".getBytes());
        get.addColumn("accountinfo".getBytes(), "type".getBytes());
        get.addColumn("accountinfo".getBytes(), "location".getBytes());
        get.addColumn("accountinfo".getBytes(), "frozen".getBytes());
        //获得返回结构
        Result result = table.get(get);
        //从返回结构提取数据
        accountInfo.setUserID(Arrays.toString(result.getRow()));
        accountInfo.setPassword(Arrays.toString(result.getValue("accountinfo".getBytes(), "password".getBytes())));
        accountInfo.setTel(Arrays.toString(result.getValue("accountinfo".getBytes(), "tel".getBytes())));
        accountInfo.setType(Arrays.toString(result.getValue("accountinfo".getBytes(), "type".getBytes())));
        accountInfo.setLocation(Arrays.toString(result.getValue("accountinfo".getBytes(), "location".getBytes())));
        accountInfo.setForzen(Arrays.toString(result.getValue("accountinfo".getBytes(), "frozen".getBytes())));
        //关闭数据库管理对象及连接
        table.close();
        con.close();
        return accountInfo;
    }

    public static boolean updateIcon(String USERID, String path) throws IOException {
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库管理对象
        Admin conAdmin = con.getAdmin();

        //获取数据库表
        Table table = con.getTable(TableName.valueOf("user"));
        Put put = new Put(USERID.getBytes());
        put.addColumn("userinfo".getBytes(), "icon".getBytes(), path.getBytes());
        table.put(put);

        table.close();
        con.close();
        return true;
    }

    public static boolean updateInfo(AccountInfo account, String username) throws IOException {
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库管理对象
        Admin conAdmin = con.getAdmin();

        //获取数据库表
        Table table = con.getTable(TableName.valueOf("user"));
        Put put = new Put(account.getUserID().getBytes());
        put.addColumn("userinfo".getBytes(), "username".getBytes(), username.getBytes());
        put.addColumn("accountinfo".getBytes(), "location".getBytes(), account.getLocation().getBytes());
        put.addColumn("accountinfo".getBytes(), "tel".getBytes(), account.getTel().getBytes());
        table.put(put);

        table.close();
        con.close();

        return true;
    }


    public UserInfo getIcon(String USERID) throws IOException {
        UserInfo userInfo = new UserInfo();
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        Table table = con.getTable(TableName.valueOf("user"));

        Get get = new Get(USERID.getBytes());
        get.addFamily("userinfo".getBytes());
        Result result = table.get(get);
        userInfo.setUsername(Bytes.toString(result.getValue("userinfo".getBytes(), "username".getBytes())));
        userInfo.setIcon(Arrays.toString(result.getValue("userinfo".getBytes(), "icon".getBytes())));

        table.close();
        con.close();
        return userInfo;
    }
}
