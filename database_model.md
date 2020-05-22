## 数据库表格

1. accountinfo：账户信息表
   1. userID：序列类型，作为代理键，由数据库分配，用户输入信息之后作为返回值告知用户用户名，提醒妥善保存。
   2. password：用户密码，8-16位。
   3. tel：电话，11位。（用户、商家可用）
   4. type：账户类型（用户，商家，管理员）。
   5. location：地址，用于送餐使用。（用户、商家可用）
   6. forzen：冻结时间，仅麻一琪使用，其他人不需要建立此属性
2. storeinfo：商铺信息
   1. userID：商家ID
   2. turnover：成交量
3. userinfo：用户信息
   1. username：用户名
   2. icon：头像
4. dish：菜品
   1. dishID：菜品编号，代理健。类似userID。
   2. userID：商家ID，外键。
   3. dishname：菜品名称。
   4. number：数量。
   5. price：价格。
   6. image：图片。
   7. content：详细内容。
   8. turnover：销量。
5. orderform：订单
   1. orderformID：订单编号，代理健。类似userID。
   2. dishID：菜品编号，外键
   3. number：菜品数量
   4. price：总价
   5. userID：用户ID
   6. storeID：商家ID
   7. status：订单状态
   8. date：订单日期
   9. fromlocation：发货地址
   10. tolocation：接货地址
   11. content：备注

## 小程序端：

展示菜品信息：dishname：菜品名称。price：价格。image：图片。content：详细内容。turnover：销量。

展示订单信息：image：图片。dishname：菜品名称。price：总价。storename：商家名称。status：订单状态。date：订单日期。

展示订单详细信息：image：图片。orderformID：订单编号。dishname：菜品名称。number：菜品数量。price：总价。storename：商家名称。status：订单状态。date：订单日期。fromlocation：发货地址。tolocation：接货地址。tel：接货人电话。content：备注。

展示个人信息：username：用户名。icon：头像。

## web端：

要求输入的菜品信息内容：dishname：菜品名称。price：价格。image：图片。content：详细内容。number：数量。

可以修改的菜品信息内容同上。

可以删除的菜品信息内容同上。

待接受订单显示内容：image：图片。orderformID：订单编号。dishname：菜品名称。number：菜品数量。price：总价。status：订单状态。date：订单日期。tolocation：接货地址。tel：接货人电话。content：备注。

已完成订单显示内容同上。

管理员可查看的用户信息显示：username：用户名。icon：头像。userID：用户账号。