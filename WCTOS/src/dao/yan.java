package dao;

import domain.Dish;
import domain.Order;
import domain.OrderFrom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class yan {
    public static Dish GetDish(String dishID) throws IOException {
        Dish dish = new Dish();
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);

        //获取数据库表
        Table dishinfo = con.getTable(TableName.valueOf("dish"));


        //配置Get对象，定义需要返回的结构
        Get get = new Get(dishID.getBytes());
        get.addColumn("dishinfo".getBytes(), "userID".getBytes());
        get.addColumn("dishinfo".getBytes(), "dishname".getBytes());
        get.addColumn("dishinfo".getBytes(), "number".getBytes());
        get.addColumn("dishinfo".getBytes(), "price".getBytes());
        get.addColumn("dishinfo".getBytes(), "image".getBytes());
        get.addColumn("dishinfo".getBytes(), "content".getBytes());
        get.addColumn("dishinfo".getBytes(), "turnover".getBytes());

        //获得返回结构
        Result result = dishinfo.get(get);

        dish.setDishID(new String(result.getRow()));
        dish.setUserID(Bytes.toString(result.getValue("dishinfo".getBytes(), "userID".getBytes())));
        dish.setDishName(Bytes.toString(result.getValue("dishinfo".getBytes(), "dishname".getBytes())));
        dish.setPrice(Bytes.toDouble(result.getValue("dishinfo".getBytes(),"price".getBytes())));
        dish.setImage(Bytes.toString(result.getValue("dishinfo".getBytes(), "image".getBytes())));
        dish.setContent(Bytes.toString(result.getValue("dishinfo".getBytes(), "content".getBytes())));
        dish.setTurnover(Bytes.toInt(result.getValue("dishinfo".getBytes(),"turnover".getBytes())));
        //关闭数据库管理对象及连接
        dishinfo.close();
        con.close();

        return dish;
    }

    public static Order Orderform(String orderformID) throws IOException {
        Order order = new Order();
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);

        //获取数据库表
        Table orderform = con.getTable(TableName.valueOf("orderform"));


        //配置Get对象，定义需要返回的结构
        Get get = new Get(orderformID.getBytes());
        get.addColumn("orderinfo".getBytes(),"dishID".getBytes());
        get.addColumn("orderinfo".getBytes(),"number".getBytes());
        get.addColumn("orderinfo".getBytes(),"price".getBytes());
        get.addColumn("orderinfo".getBytes(),"userID".getBytes());
        get.addColumn("orderinfo".getBytes(),"storeID".getBytes());
        get.addColumn("orderinfo".getBytes(),"status".getBytes());
        get.addColumn("orderinfo".getBytes(),"date".getBytes());
        get.addColumn("orderinfo".getBytes(),"fromlocation".getBytes());
        get.addColumn("orderinfo".getBytes(),"tolocation".getBytes());
        get.addColumn("orderinfo".getBytes(),"content".getBytes());

        //获得返回结构
        Result result = orderform.get(get);
        order.setOrderFormID(Bytes.toString(result.getRow()));
        order.setDishID(Bytes.toString(result.getValue("orderinfo".getBytes(),"dishID".getBytes())));
        order.setNumber(Bytes.toInt(result.getValue("orderinfo".getBytes(),"number".getBytes())));
        order.setPrice(Bytes.toDouble(result.getValue("orderinfo".getBytes(),"price".getBytes())));
        order.setUserID(Bytes.toString(result.getValue("orderinfo".getBytes(),"userID".getBytes())));
        order.setStoreID(Bytes.toString(result.getValue("orderinfo".getBytes(),"storeID".getBytes())));
        order.setStatus(Bytes.toString(result.getValue("orderinfo".getBytes(),"status".getBytes())));
        order.setDate(Bytes.toString(result.getValue("orderinfo".getBytes(),"date".getBytes())));
        order.setFromLocation(Bytes.toString(result.getValue("orderinfo".getBytes(),"fromlocation".getBytes())));
        order.setToLocation(Bytes.toString(result.getValue("orderinfo".getBytes(),"tolocation".getBytes())));
        order.setContent(Bytes.toString(result.getValue("orderinfo".getBytes(),"content".getBytes())));

        orderform.close();
        con.close();
        return order;
    }

    public static List<Dish> GetDishList() throws IOException {
        List<Dish> list = new ArrayList<Dish>();
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库表
        Table table = con.getTable(TableName.valueOf("dish"));
        Scan scan = new Scan();
        scan.addFamily("dishinfo".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Iterator<Result> results = resultScanner.iterator();

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
        }

        return list;
    }

    public static List<OrderFrom> GetOrderformList() throws IOException {
        List<OrderFrom> list = new ArrayList<OrderFrom>();
        //配置数据库连接
        Configuration conf = new Configuration();
        //测试注意在hosts文件添加映射 master 49.234.227.49
        conf.set("hbase.zookeeper.quorum", "master");
        //获取数据库连接
        Connection con = ConnectionFactory.createConnection(conf);
        //获取数据库表
        Table table = con.getTable(TableName.valueOf("orderform"));
        Scan scan = new Scan();
        scan.addFamily("orderinfo".getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        Iterator<Result> results = resultScanner.iterator();

        OrderFrom orderFrom;
        Result result;
        while (results.hasNext()) {
            orderFrom = new OrderFrom();
            result = results.next();
            for (Cell cell : result.rawCells()) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                orderFrom.setOrderFormID(row);
                if (colName.equals("dishID")) {
                    orderFrom.setDishID(value);
                }
                if (colName.equals("number")) {
                    orderFrom.setNumber(Integer.parseInt(value));
                }
                if (colName.equals("price")) {
                    orderFrom.setPrice(Double.parseDouble(value));
                }
                if (colName.equals("userID")) {
                    orderFrom.setUserID(value);
                }
                if (colName.equals("storeID")) {
                    orderFrom.setStoreID(value);
                }
                if (colName.equals("status")) {
                    orderFrom.setStatus(value);
                }
                if (colName.equals("date")) {
                    orderFrom.setDate(value);
                }
                if (colName.equals("fromlocation")) {
                    orderFrom.setFromLocation(value);
                }
                if (colName.equals("tolocation")) {
                    orderFrom.setToLocation(value);
                }
                if (colName.equals("content")) {
                    orderFrom.setContent(value);
                }
                list.add(orderFrom);
            }
        }
        return list;
    }
}
