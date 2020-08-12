package dao;

import domain.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

public class liu {
    public static User find(String userID) throws Exception {
        User user = new User();
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库管理对象
        Admin conAdmin = con.getAdmin();

//        新建表
//        HTableDescriptor table = new HTableDescriptor("user".getBytes());
//        HColumnDescriptor family = new HColumnDescriptor("userinfo".getBytes());
//        table.addFamily(family);
//        conAdmin.createTable(table);

        //获取数据库表
        Table userinfo = con.getTable(TableName.valueOf("user"));

//        插入数据
//        Put put=new Put("001".getBytes());
//        put.addColumn("userinfo".getBytes(),"username".getBytes(),"mayiqi".getBytes());
//        put.addColumn("userinfo".getBytes(),"icon".getBytes(),"123456".getBytes());
//        userinfo.put(put);

        //配置Get对象，定义需要返回的结构
        Get get = new Get(userID.getBytes());
        get.addColumn("userinfo".getBytes(), "username".getBytes());
        get.addColumn("userinfo".getBytes(), "icon".getBytes());
        //获得返回结构
        Result result = userinfo.get(get);
        user.setUsername(new String(result.getValue("userinfo".getBytes(), "username".getBytes())));
        user.setIcon(Arrays.toString(result.getValue("userinfo".getBytes(), "icon".getBytes())));

        //关闭数据库管理对象及连接
        conAdmin.close();
        con.close();

        return user;
    }

    public static boolean delete(String userID) throws Exception {
        //配置数据库连接
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库管理对象
        Admin conAdmin = con.getAdmin();

        //获取数据库表
        Table userinfo = con.getTable(TableName.valueOf("user"));
        //删除数据
        Delete delete = new Delete(userID.getBytes());
        userinfo.delete(delete);

//        Scan scan=new Scan();
//        ResultScanner result=userinfo.getScanner(scan);//得到的是数据集
//        Result str=null;
//        while((str=result.next())!=null){
//            List<Cell> cells = str.listCells();//将每条数据存到Cell对象里
//            for (Cell c:cells) {
//                System.out.println(new String(CellUtil.cloneFamily(c))+new String(CellUtil.cloneQualifier(c))+new String(CellUtil.cloneValue(c)));
//            }
//        }

        //关闭数据库管理对象及连接
        conAdmin.close();
        con.close();
        return true;
    }

    public static List<User> listAll() throws Exception {

        List<User> list = new ArrayList<User>();
        User user;
        //配置数据库连接
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库管理对象
        Admin conAdmin = con.getAdmin();

        //获取数据库表
        Table userinfo = con.getTable(TableName.valueOf("user"));

//        //插入数据
//        Put put=new Put("001".getBytes());
//        put.addColumn("userinfo".getBytes(),"username".getBytes(),"mayiqi".getBytes());
//        put.addColumn("userinfo".getBytes(),"icon".getBytes(),"123456".getBytes());
//        userinfo.put(put);
//        Put put1=new Put("002".getBytes());
//        put1.addColumn("userinfo".getBytes(),"username".getBytes(),"mayiqi".getBytes());
//        put1.addColumn("userinfo".getBytes(),"icon".getBytes(),"123456".getBytes());
//        userinfo.put(put1);

        //查询所有数据
        Scan scan = new Scan();
        ResultScanner result = userinfo.getScanner(scan);//得到的是数据集
        //将数据按行送入user对象，形成user列表
        for (Result r : result) {
            user = new User();
            for (Cell cell : r.rawCells()) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                user.setUserID(row);
                if (colName.equals("username")) {
                    user.setUsername(value);
                }
                if (colName.equals("icon")) {
                    user.setIcon(Arrays.toString(value.getBytes()));
                }
            }
            list.add(user);
        }

        //关闭数据库管理对象及连接
        conAdmin.close();
        con.close();

        return list;
    }

    public static boolean update(User user) throws Exception {
        //配置数据库连接
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库管理对象
        Admin conAdmin = con.getAdmin();

        //获取数据库表
        Table userinfo = con.getTable(TableName.valueOf("user"));

        Put put=new Put(user.getUserID().getBytes());
        put.addColumn("userinfo".getBytes(),"username".getBytes(),user.getUsername().getBytes());
        put.addColumn("userinfo".getBytes(),"icon".getBytes(),user.getIcon().getBytes());
        userinfo.put(put);

        //关闭数据库管理对象及连接
        conAdmin.close();
        con.close();

        return true;
    }
}
