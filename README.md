# redishelper
## 设计理念
Redis中存储的都是Key/Value的数据，常规的Redis操作类库对Redis的操作都是对key-value的直接操作，这样容易产生两个问题：第一、
key的拼接在代码中随从可见，严重降低了代码的可读性；第二、由于key的分隔符使用多种多样，不容易规范。

本类库设计时参考了 [阿里云Redis开发规范](https://developer.aliyun.com/article/531067)，将规范和要求融入到组件中，从而很好的解决了以上两个问题。

为了让Redis的使用更符合使用传统关系型数据库使用方式，本组件中引入数据库、表的概念，可以创建数据库，然后在数据库中创建表，对数据的操作均在表中进行。



## 使用方式
Maven引入：
```xml
<dependency>
    <groupId>com.xiaotao.redis</groupId>
    <artifactId>redis-helper</artifactId>
    <version>LATEST</version>
</dependency>
```

代码实现示例：
```java
// 创建数据库
RedisDatabase database = new RedisDatabase("dbname", redisConnectionFactory);

// 创建表
RedisTable redisTable = database.createTable("demo");

// 删除数据库
database.delete();

// 删除表
redisTable.delete();
```

各数据类型的操作示例如下：
```java
// String
redisTable.valueOps().set("name1", "zhangsan");
String name = redisTable.valueOps().get("name1");

// Hash
redisTable.hashOps().getAll("key1");

// List
redisTable.listOps().size("key2");

// Set
redisTable.setOps().members("key3");

// ZSet
redisTable.zSetOps().size("key4");
```

过期时间的支持：
目前支持滑动窗口过期和普通过期(一次性过期)两种方式。
```java
// 对表设置滑动窗口过期，对该表中增加的数据都会设置滑动窗口过期，从而不需要再对加入得数据单独设置
redisTable.setAutoWindow(10, TimeUnit.SECONDS)

// 对表设置普通过期,对该表中增加的数据都会自动设置过期时间，从而不需要再对加入得数据单独设置
redisTable.setExpire(10, TimeUnit.SECONDS)

// 也可以对表中的某条数据单独设置过期时间，该过期时间会覆盖对表的设置，仅支持普通过期时间的设置
redisTable.valueOps().expire("key1", 10, TimeUnit.SECONDS)
redisTable.valueOps().expireAt("key1", new date())
```