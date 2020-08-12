package dao;

import domain.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class tang {
    public static boolean restLogin(String userID, String password) throws IOException {
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
        Get get = new Get(userID.getBytes());
        get.addColumn("accountinfo".getBytes(), "password".getBytes());
        Result result = table.get(get);
        String re_Password = new String(result.getValue("accountinfo".getBytes(), "password".getBytes()));

        //关闭数据库管理对象及连接
        conAdmin.close();
        con.close();
        return re_Password.equals(password);
    }

    public static boolean restRegister(Restaurant restaurant) throws IOException {
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
        Get get = new Get(restaurant.getUserID().getBytes());
        try {
            table.get(get);
        } catch (IOException e) {
            //数据库中没有此用户，写入此用户信息
            Put put = new Put(restaurant.getUserID().getBytes());
            put.addColumn("accountinfo".getBytes(), "password".getBytes(), restaurant.getPassword().getBytes());
            put.addColumn("accountinfo".getBytes(), "tel".getBytes(), restaurant.getTel().getBytes());
            put.addColumn("accountinfo".getBytes(), "type".getBytes(), restaurant.getType().getBytes());
            put.addColumn("accountinfo".getBytes(), "location".getBytes(), restaurant.getLocation().getBytes());
            table.put(put);

            //关闭数据库管理对象及连接
            table.close();
            conAdmin.close();
            con.close();
            return true;
        }

        //关闭数据库管理对象及连接
        conAdmin.close();
        con.close();

        return false;
    }

    public static List<Dish> queryALllDish(String userID) throws IOException {
        List<Dish> list = new ArrayList<Dish>();
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库管理对象
        Admin conAdmin = con.getAdmin();

        //获取数据库表
        Table table = con.getTable(TableName.valueOf("dish"));
        Scan scan = new Scan();
        //新建比较器
        BinaryComparator comparator = new BinaryComparator(userID.getBytes());
        //新建过滤器
        SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter("dishinfo".getBytes(), "userID".getBytes(), CompareFilter.CompareOp.EQUAL, comparator);
        scan.setFilter(filter);
        ResultScanner tableScanner = table.getScanner(scan);
        //返回迭代器
        Iterator<Result> results = tableScanner.iterator();

        Dish dish;
        Result result;
        while (results.hasNext()) {
            dish = new Dish();
            result = results.next();
            for (Cell cell : result.rawCells()) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                dish.setDishID(row);
                if (colName.equals("userID")) {
                    dish.setUserID(value);
                }
                if (colName.equals("dishName")) {
                    dish.setDishName(value);
                }
                if (colName.equals("number")) {
                    dish.setNumber(Integer.parseInt(value));
                }
                if (colName.equals("price")) {
                    dish.setPrice(Double.parseDouble(value));
                }
                if (colName.equals("image")) {
                    dish.setImage(Arrays.toString(value.getBytes()));
                }
                if (colName.equals("content")) {
                    dish.setContent(value);
                }
                if (colName.equals("turnover")) {
                    dish.setTurnover(Integer.parseInt(value));
                }
                list.add(dish);
            }
            //关闭数据库管理对象及连接
            table.close();
            tableScanner.close();
            conAdmin.close();
            con.close();
        }
        return list;
    }

    public static boolean addDish(String userID, Dish newDish) throws IOException {
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库管理对象
        Admin conAdmin = con.getAdmin();
        //获取数据库表
        Table table = con.getTable(TableName.valueOf("dish"));
        Put put = new Put(newDish.getDishID().getBytes());
        put.addColumn("dishinfo".getBytes(), "userID".getBytes(), newDish.getUserID().getBytes());
        put.addColumn("dishinfo".getBytes(), "dishname".getBytes(), newDish.getDishName().getBytes());
        put.addColumn("dishinfo".getBytes(), "number".getBytes(), Bytes.toBytes(newDish.getNumber()));
        put.addColumn("dishinfo".getBytes(), "price".getBytes(), Bytes.toBytes(newDish.getPrice()));
        put.addColumn("dishinfo".getBytes(), "image".getBytes(), newDish.getImage().getBytes());
        put.addColumn("dishinfo".getBytes(), "content".getBytes(), newDish.getContent().getBytes());
        put.addColumn("dishinfo".getBytes(), "turnover".getBytes(), Bytes.toBytes(newDish.getTurnover()));
        table.put(put);

        table.close();
        conAdmin.close();
        con.close();
        return true;
    }

    public static List<Order> queryAllOrder(String userID) throws IOException {
        List<Order> list = new ArrayList<Order>();
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库管理对象
        Admin conAdmin = con.getAdmin();
        //获取数据库表
        Table table = con.getTable(TableName.valueOf("orderfrom"));
        Scan scan = new Scan();
        //新建比较器
        BinaryComparator comparator = new BinaryComparator(userID.getBytes());
        //新建过滤器
        SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter("orderinfo".getBytes(), "storeID".getBytes(), CompareFilter.CompareOp.EQUAL, comparator);
        scan.setFilter(filter);
        ResultScanner tableScanner = table.getScanner(scan);
        //返回迭代器
        Iterator<Result> results = tableScanner.iterator();

        Order order;
        Result result;
        while (results.hasNext()) {
            order = new Order();
            result = results.next();
            for (Cell cell : result.rawCells()) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                order.setOrderFormID(row);
                if (colName.equals("userID")) {
                    order.setUserID(value);
                }
                if (colName.equals("dishID")) {
                    order.setDishID(value);
                }
                if (colName.equals("number")) {
                    order.setNumber(Integer.parseInt(value));
                }
                if (colName.equals("price")) {
                    order.setPrice(Double.parseDouble(value));
                }
                if (colName.equals("storeID")) {
                    order.setStoreID(value);
                }
                if (colName.equals("status")) {
                    order.setStatus(value);
                }
                if (colName.equals("date")) {
                    order.setContent(value);
                }
                if (colName.equals("fromLocation")) {
                    order.setFromLocation(value);
                }
                if (colName.equals("toLocation")) {
                    order.setToLocation(value);
                }
                if (colName.equals("content")) {
                    order.setContent(value);
                }
                list.add(order);
            }
        }
        table.close();
        conAdmin.close();
        con.close();

        return list;
    }

    //返回值为true，代表删除成功
//返回值为flase，代表删除失败，可能原因：该商家不拥有此菜品
    public static boolean delDish(String userID, String dishName) throws IOException {
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库管理对象
        Admin conAdmin = con.getAdmin();
        //获取数据库表
        Table table = con.getTable(TableName.valueOf("dish"));
        Scan scan = new Scan();
        //新建比较器
        BinaryComparator comparator = new BinaryComparator(userID.getBytes());
        //新建过滤器
        SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter("dishinfo".getBytes(), "userID".getBytes(), CompareFilter.CompareOp.EQUAL, comparator);
        scan.setFilter(filter);
        ResultScanner tableScanner = table.getScanner(scan);
        //返回迭代器
        Iterator<Result> results = tableScanner.iterator();
        Result result;
        while (results.hasNext()) {
            result = results.next();
            for (Cell cell : result.rawCells()) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                if (colName.equals("dishname")) {
                    if (value.equals(dishName)) {
                        //删除dishname列的值等于dishName的一行
                        Delete delete = new Delete(row.getBytes());
                        table.delete(delete);

                        table.close();
                        conAdmin.close();
                        con.close();

                        return true;
                    }
                }
            }
        }

        table.close();
        conAdmin.close();
        con.close();

        return false;
    }

    public static boolean modifyDish(Dish dish) throws IOException {
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库管理对象
        Admin conAdmin = con.getAdmin();
        //获取数据库表
        Table table = con.getTable(TableName.valueOf("dish"));
        Put put = new Put(dish.getDishID().getBytes());
        put.addColumn("dishinfo".getBytes(), "userID".getBytes(), dish.getUserID().getBytes());
        put.addColumn("dishinfo".getBytes(), "dishname".getBytes(), dish.getDishName().getBytes());
        put.addColumn("dishinfo".getBytes(), "number".getBytes(), Bytes.toBytes(dish.getNumber()));
        put.addColumn("dishinfo".getBytes(), "price".getBytes(), Bytes.toBytes(dish.getPrice()));
        put.addColumn("dishinfo".getBytes(), "image".getBytes(), dish.getImage().getBytes());
        put.addColumn("dishinfo".getBytes(), "content".getBytes(), dish.getContent().getBytes());
        put.addColumn("dishinfo".getBytes(), "turnover".getBytes(), Bytes.toBytes(dish.getTurnover()));
        table.put(put);

        table.close();
        conAdmin.close();
        con.close();

        return true;
    }

    public static Account basicInfo(String userID) throws Exception {
        Account account = new Account();
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库管理对象
        Admin conAdmin = con.getAdmin();

        //获取数据库表
        Table userinfo = con.getTable(TableName.valueOf("user"));

        //配置Get对象，定义需要返回的结构
        Get get = new Get(userID.getBytes());
        get.addColumn("accountinfo".getBytes(), "password".getBytes());
        get.addColumn("accountinfo".getBytes(), "tel".getBytes());
        get.addColumn("accountinfo".getBytes(), "type".getBytes());
        get.addColumn("accountinfo".getBytes(), "location".getBytes());
        get.addColumn("accountinfo".getBytes(), "frozen".getBytes());
        //获得返回结构
        Result result = userinfo.get(get);
        //从返回结构提取数据
        account.setUserID(Arrays.toString(result.getRow()));
        account.setPassword(Arrays.toString(result.getValue("accountinfo".getBytes(), "password".getBytes())));
        account.setTel(Arrays.toString(result.getValue("accountinfo".getBytes(), "tel".getBytes())));
        account.setType(Arrays.toString(result.getValue("accountinfo".getBytes(), "type".getBytes())));
        account.setLocation(Arrays.toString(result.getValue("accountinfo".getBytes(), "location".getBytes())));
        account.setForzen(Arrays.toString(result.getValue("accountinfo".getBytes(), "frozen".getBytes())));
        //关闭数据库管理对象及连接
        conAdmin.close();
        con.close();

        return account;
    }

    public static boolean changePwd(String userID, String newPassword) throws IOException {
        //如果新输入密码与原密码相同，则返回false
        if (tang.restLogin(userID, newPassword)) {
            return false;
        }
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库管理对象

        //获取数据库表
        Table userinfo = con.getTable(TableName.valueOf("user"));
        Put put = new Put(userID.getBytes());
        put.addColumn("accountinfo".getBytes(),"password".getBytes(),newPassword.getBytes());
        userinfo.put(put);

        userinfo.close();
        con.close();
        return true;
    }

}
