# RabbitMQ



###  常见MQ产品

- ​	ActiveMQ：基于JMS
- ​	RabbitMQ：基于AMQP协议，erlang语言开发，稳定性好
- ​	RocketMQ：基于JMS，阿里巴巴产品，目前交由Apache基金会
- ​	Kafka：分布式消息系统，高吞吐量
- ​	ZeroMQ，IBM WebSphere



#  七种消息模型

RabbitMQ提供了7种消息模型

- 1、2是队列 模型
- 3、4、5是订阅，交换机模型
- 6、是rpc回调模型
- 7、是确认模型



#### 新建demo项目

#### pom.xml

```
<parent> 
    <groupId>org.springframework.boot</groupId> 
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>2.3.1.RELEASE</version> 
</parent>

<properties> 
	<java.version>1.8</java.version> 
</properties>

<dependencies> 
    <dependency> 
        <groupId>org.apache.commons</groupId> 
        <artifactId>commons-lang3</artifactId> 
    </dependency> 

    <dependency> 
        <groupId>org.springframework.boot</groupId> 
        <artifactId>spring-boot-starter-amqp</artifactId> 
    </dependency> 

    <dependency> 
        <groupId>org.springframework.boot</groupId> 
        <artifactId>spring-boot-starter-test</artifactId> 
    </dependency> 
</dependencies>
```

#### 新建包com.mr.rabbitmq.utils

#### 在utils包下新建RabbitmqConnectionUtil

```
public class RabbitmqConnectionUtil { 
	public static Connection getConnection() throws Exception { 
        //定义rabbitmq连接工厂 
        ConnectionFactory factory = new ConnectionFactory(); 
        //设置超时时间 
        factory.setConnectionTimeout(60000); 
        //设置服务ip 
        factory.setHost("127.0.0.1"); 
        //设置端口5672 
        factory.setPort(5672); 
        //设置，用户名、密码、虚拟主机 
        factory.setUsername("guest"); 
        factory.setPassword("guest"); 
        // 创建连接，根据工厂 
        Connection connection = factory.newConnection(); 
        return connection; 
	} 
}
```

##  基本消息模型

在com.mr.rabbitmq下新建simple包

包下新建SendMessage

```
    //序列名称 
    private final static String QUEUE_NAME = "simple_queue"; 

    //主函数 
    public static void main(String[] arg) throws Exception { 
        // 获取到连接 
        Connection connection = RabbitmqConnectionUtil.getConnection(); 
        // 获取通道 
        Channel channel = connection.createChannel(); 
        /*param1:队列名称 
        param2: 是否持久化 
        param3: 是否排外 
        param4: 是否自动删除 
        param5: 其他参数 
        */ 
        channel.queueDeclare(QUEUE_NAME, false, false, false, null); 
        //发送的消息内容 
        String message = "good good study"; 
        /*param1: 交换机名称 
        param2: routingKey 
        param3: 一些配置信息 
        param4: 发送的消息
        */ 
        //发送消息
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes()); 
        System.out.println(" 消息发送 '" + message + "' 到队列 success"); 
        // 关闭通道和连接 
        channel.close(); 
        connection.close(); 
    }
```

- param1: 队列名字

- param2: durable

  ​	是否持久化

  ​	是否持久化, 队列的声明默认是存放到内存中的，如果rabbitmq重启会丢失，如果想重启之

  ​	后还存在就要使队列持久化，保存到Erlang自带的Mnesia数据库中，当rabbitmq重启之后会

  ​	读取该数据库

- param3: exclusive

  ​	是否排外

  ​	是否排外的，有两个作用，一：当连接关闭时connection.close()该队列是否会自动删除；

  ​		二：该队列是否是私有的private，如果不是排外的，可以使用两个消费者都访问同一个队

  ​		列，没有任何问题，如果是排外的，会对当前队列加锁，其他通道channel是不能访问的，如

  ​		果强制访问会报异常： 一般等于true的话用于一个队列只能有一个消费者来消费的场景

​		队列只能有一个消费者来消费的场景

- param4 : autoDelete

  ​	是否自动删除队列，当最后一个消费者断开连接之后队列是否自动被删除，可以通过

  ​	RabbitMQ Management，查看某个队列的消费者数量，当consumers = 0时队列就会自动删除

- param5: 相关参数

在simple包下新建Receive

```
public class Receive { 
    //队列名称 
    private final static String QUEUE_NAME = "simple_queue"; 
    public static void main(String[] arg) throws Exception { 
        // 获取连接 
        Connection connection = RabbitmqConnectionUtil.getConnection(); 
        // 创建通道 
        Channel channel = connection.createChannel(); 
        // 声明队列 
        channel.queueDeclare(QUEUE_NAME, false, false, false, null); 
        // 定义队列 接收端==》消费者
        DefaultConsumer consumer = new DefaultConsumer(channel) { 
            // 监听队列中的消息，如果有消息，进行处理 
            @Override public void handleDelivery(String consumerTag, Envelope envelope, 					AMQP.BasicProperties properties, 
                    byte[] body) throws IOException { 
            // body： 消息中参数信息 
            String msg = new String(body); 
            System.out.println(" 收到消息，执行中 : " + msg + "!"); 
		} 
	}; 
	/*param1 : 队列名称 
	param2 : 是否自动确认消息 
	param3 : 消费者 
	*/
        channel.basicConsume(QUEUE_NAME, true, consumer); 
        //消费者需要时时监听消息，不用关闭通道与连接 
	} 
}
```

## work 消息模型

在刚才的基本模型中，一个生产者，一个消费者，生产的消息直接被消费者消费。比较简单。

Work queues，也被称为（Task queues），任务模型。

当消息处理比较耗时的时候，可能生产消息的速度会远远大于消息的消费速度。长此以往，消息就会堆

积越来越多，无法及时处理。此时就可以使用work 模型：**让多个消费者绑定到一个队列，共同消费队**

**列中的消息**。队列中的消息一旦消费，就会消失，因此任务是不会被重复执行的。

![](C:\Users\86156\Pictures\Camera Roll\屏幕截图 2021-03-11 213006.png)



角色：

​	P：生产者：任务的发布者

​	C1：消费者，领取任务并且完成任务，假设完成速度较慢

​	C2：消费者2：领取任务并完成任务，假设完成速度快

### 生产者

在com.mr.rabbitmq下新建包work

SendMessage

```
public class SendMessage { 
	//序列名称 
	private final static String QUEUE_NAME = "test_work_queue"; 
    //主函数 
    public static void main(String[] arg) throws Exception { 
        // 获取到连接 
        Connection connection = RabbitmqConnectionUtil.getConnection(); 
        // 获取通道 
        Channel channel = connection.createChannel();
        /*param1:队列名称 
          param2: 是否持久化 
          param3: 是否排外 
          param4: 是否自动删除 
          param5: 其他参数 
         */ 
         channel.queueDeclare(QUEUE_NAME, false, false, false, null); 
         // 循环发送消息100条 
         for (int i = 0; i < 100; i++) { 
             // 消息参数内容 
             String message = "task - good study -" + i; 
             /*param1: 交换机名称 
             param2: routingKey 
             param3: 一些配置信息 
             param4: 发送的消息 
             */ 
             channel.basicPublish("", QUEUE_NAME, null, message.getBytes()); 						 System.out.println(" send '" + message + "' success"); 
         }
         // 关闭通道和连接 
         channel.close(); 
         connection.close(); 
      } 
  }
```

###  消费者

在work包下新建ReceiveOne

```
public class ReceiveOne { 
    //队列名称 
    private final static String QUEUE_NAME = "test_work_queue"; 
    public static void main(String[] arg) throws Exception { 
        // 获取连接 
        Connection connection = RabbitmqConnectionUtil.getConnection();
        // 创建通道 
        Channel channel = connection.createChannel(); 
        // 声明队列 
        channel.queueDeclare(QUEUE_NAME, false, false, false, null); 
        // 定义队列 接收端==》消费者 
        DefaultConsumer consumer = new DefaultConsumer(channel) { 
            // 监听队列中的消息，如果有消息，进行处理 
            @Override 
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, 
            byte[] body) throws IOException { 
            // body： 消息中参数信息 
            String msg = new String(body); 
            System.out.println(" [消费者-1] 收到消息 : " + msg ); 
            //System.out.println(1/0); 
            /*param1 : （唯一标识 ID） 
            param2 : 是否进行批处理 
            */ 
            channel.basicAck(envelope.getDeliveryTag(), false); 
          } 
       }; 
       /*param1 : 队列名称 
       param2 : 是否自动确认消息 
       param3 : 消费者 
       */
       channel.basicConsume(QUEUE_NAME, false, consumer); 
       //消费者需要时时监听消息，不用关闭通道与连接 
     }
  }
```

### 消费者2

在work包下新建ReceiveTwo

```
public class ReceiveOne { 
    //队列名称 
    private final static String QUEUE_NAME = "test_work_queue"; 
    public static void main(String[] arg) throws Exception { 
        // 获取连接 
        Connection connection = RabbitmqConnectionUtil.getConnection();
        // 创建通道 
        Channel channel = connection.createChannel(); 
        // 声明队列 
        channel.queueDeclare(QUEUE_NAME, false, false, false, null); 
        // 定义队列 接收端==》消费者 
        DefaultConsumer consumer = new DefaultConsumer(channel) { 
            // 监听队列中的消息，如果有消息，进行处理 
            @Override 
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, 
            byte[] body) throws IOException { 
            // body： 消息中参数信息 
            String msg = new String(body); 
            System.out.println(" [消费者-2] 收到消息 : " + msg ); 
            //System.out.println(1/0); 
            try {
            //增加消费者2消费消息的时间 
            	Thread.sleep(1000); 
            } catch (InterruptedException e) { 
            	e.printStackTrace(); 
            }
            channel.basicAck(envelope.getDeliveryTag(), false); 
           }
        };
       /*param1 : 队列名称 
       param2 : 是否自动确认消息 
       param3 : 消费者 
       */
       channel.basicConsume(QUEUE_NAME, false, consumer); 
       //消费者需要时时监听消息，不用关闭通道与连接 
     }
  }
```

运行ReceiveOne和ReceiveTwo

再运行SendMessage

### 能者多劳

刚才的实现有问题吗？

消费者2比消费者1的效率要低，一次任务的耗时较长

然而两人最终消费的消息数量是一样的

消费者1大量时间处于空闲状态，消费者2一直忙碌

现在的状态属于是把任务平均分配，正确的做法应该是消费越快的人，消费的越多。

怎么实现呢？

我们可以修改设置，让消费者同一时间只接收一条消息，这样处理完成之前，就不会接收更多消息，就

可以让处理快的人，接收更多消息 ：

消费者增加代码，设置获取一条消息，执行完在执行下一次

```
//通道设置 一个消费者获取一条消息，执行完毕，再获取下一条 
channel.basicQos(1);
```

## 订阅模型

前面2个案例中，只有3个角色：

P：生产者，也就是要发送消息的程序

C：消费者：消息的接受者，会一直等待消息到来。

queue：消息队列，图中红色部分。类似一个邮箱，可以缓存消息；生产者向其中投递消息，消费

者从其中取出消息。

而在订阅模型中，多了一个exchange角色，而且过程略有变化：

P：生产者，也就是要发送消息的程序，消息不再发送到队列中，而是发给X（交换机）

C：消费者，消息的接受者，会一直等待消息到来。

Queue：消息队列，接收消息、缓存消息。

Exchange：交换机，图中的X。一方面，接收生产者发送的消息。另一方面，知道如何处理消

息，例如递交给某个特别队列、递交给所有队列、或是将消息丢弃。到底如何操作，取决于

Exchange的类型。Exchange有以下3种类型：

Fanout（扇型交换机）：广播，将消息交给所有绑定到交换机的队列

Direct（直连交换机）：定向，把消息交给符合指定routing key 的队列

Topic（主题交换机）：通配符，把消息交给符合routing pattern（路由模式） 的队列

//router

**Exchange****（交换机）只负责转发消息，不具备存储消息的能力**，因此如果没有任何队列与Exchange绑

定，或者没有符合路由规则的队列，那么消息会丢失！



#### 订阅模型-Fanout

在广播模式下，消息发送流程是这样的：

1） 可以有多个消费者

2） 每个**消费者有自己的****queue**（队列）

3） 每个**队列都要绑定到****Exchange**（交换机）

4） **生产者发送的消息，只能发送到交换机**，交换机来决定要发给哪个队列，生产者无法决

定。

5） 交换机把消息发送给绑定过的所有队列

6） 队列的消费者都能拿到消息。实现一条消息被多个消费者消费

##### **生产者**

```
//交换机名称 
private final static String EXCHANGE_NAME = "fanout_exchange_test"; 
//主函数 
public static void main(String[] arg) throws Exception {
        // 获取到连接 
        Connection connection = RabbitmqConnectionUtil.getConnection(); 
        // 获取通道 
        Channel channel = connection.createChannel(); 

        /*param1: 交换机名称 param2: 交换机类型 */ 

        channel.exchangeDeclare(EXCHANGE_NAME,"fanout"); 
        //发送的消息内容 
        String message = "good good study"; 

        /*param1: 交换机名称 param2: routingKey param3: 一些配置信息 param4: 发送的消息 */ 
        //发送消息
        channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes()); 
        System.out.println(" 消息发送 '" + message + "' 到交换机 success"); 
        // 关闭通道和连接 
        channel.close(); 
        connection.close();
}
```

#####  消费者1

```
//交换机名称
private final static String EXCHANGE_NAME = "fanout_exchange_test"; 
//队列名称 
private final static String QUEUE_NAME = "fanout_exchange_queue_1";
public static void main(String[] arg) throws Exception { 
// 获取连接 
Connection connection = RabbitmqConnectionUtil.getConnection(); 
// 创建通道 
Channel channel = connection.createChannel(); 
// 声明队列 
channel.queueDeclare(QUEUE_NAME, false, false, false, null); 
//消息队列绑定到交换机 
/*param1: 序列名 param2: 交换机名 param3: routingKey*/

channel.queueBind(QUEUE_NAME,EXCHANGE_NAME,""); 
// 定义队列 接收端==》消费者
DefaultConsumer consumer = new DefaultConsumer(channel) { 
// 监听队列中的消息，如果有消息，进行处理 
@Override 
public void handleDelivery(String consumerTag, Envelope envelope, 
AMQP.BasicProperties properties,
byte[] body) throws IOException { 
// body： 消息中参数信息 
String msg = new String(body);
System.out.println(" [消费者-1] 收到消息 : " + msg ); 
} 
}; 
/*param1 : 队列名称 param2 : 是否自动确认消息 param3 : 消费者 *
/
channel.basicConsume(QUEUE_NAME, true, consumer); 
//消费者需要时时监听消息，不用关闭通道与连接 
} 
}
```

#####  消费者2

```
//交换机名称 
private final static String EXCHANGE_NAME = "fanout_exchange_test"; 
//队列名称 
private final static String QUEUE_NAME = "fanout_exchange_queue_2";
public static void main(String[] arg) throws Exception { 
        // 获取连接
        Connection connection = RabbitmqConnectionUtil.getConnection(); 
        // 创建通道 
        Channel channel = connection.createChannel(); 
        // 声明队列 
        channel.queueDeclare(QUEUE_NAME, false, false, false, null); 
        //消息队列绑定到交换机 
        channel.queueBind(QUEUE_NAME,EXCHANGE_NAME,"");
        // 定义队列 接收端==》消费者
        DefaultConsumer consumer = new DefaultConsumer(channel) { 
        // 监听队列中的消息，如果有消息，进行处理 
        @Override 
        public void handleDelivery(String consumerTag,
            Envelope envelope, AMQP.BasicProperties properties,
            byte[] body) throws IOException { 
        // body： 消息中参数信息 
                String msg = new String(body); 
                System.out.println(" [消费者-2] 收到消息 : " + msg );
        } 
      }; 
        /*param1 : 队列名称 param2 : 是否自动确认消息 param3 : 消费者 */
        channel.basicConsume(QUEUE_NAME, true, consumer); 
        //消费者需要时时监听消息，不用关闭通道与连接 
}
```

####  订阅模型-Direct

在Fanout模式中，一条消息，会被所有订阅的队列都消费。但是，在某些场景下，我们希望不同的消息

被不同的队列消费。这时就要用到Direct类型的Exchange。 

在Direct模型下

队列与交换机的绑定，不能是任意绑定了，而是要指定一个 RoutingKey （路由key）

消息的发送方在 向 Exchange发送消息时，也必须指定消息的 RoutingKey 。

Exchange不再把消息交给每一个绑定的队列，而是根据消息的 Routing Key 进行判断，只有队列

的 Routingkey 与消息的 Routing key 完全一致，才会接收到消息

```
//交换机名称
private final static String EXCHANGE_NAME = "direct_exchange_test"; 
//主函数 
public static void main(String[] arg) throws Exception { 
        // 获取到连接 
        Connection connection = RabbitmqConnectionUtil.getConnection();
        // 获取通道
        Channel channel = connection.createChannel(); 
        /*param1: 交换机名称 param2: 交换机类型 */ 
        channel.exchangeDeclare(EXCHANGE_NAME,"direct"); 
        //发送的消息内容 
        String message = "商品新增成功 id ： 153"; 
        //String message = "商品修改成功 id ： 153"; 
        //String message = "商品删除成功 id ： 153"; 
        /*param1: 交换机名称 param2: routingKey param3: 一些配置信息 param4: 发送的消息 */ 
        //发送消息 
        channel.basicPublish(EXCHANGE_NAME, "save", null, message.getBytes()); //channel.basicPublish(EXCHANGE_NAME, "update", null, message.getBytes()); //channel.basicPublish(EXCHANGE_NAME, "delete", null, message.getBytes());
        System.out.println(" [商品服务] 发送消息routingKey ：save '" + message ); 
        // 关闭通道和连接 
        channel.close(); connection.close(); }
```

##### 消费者1

```
//交换机名称 
private final static String EXCHANGE_NAME = "direct_exchange_test"; 
//队列名称 
private final static String QUEUE_NAME = "direct_exchange_queue_1"; 
public static void main(String[] arg) throws Exception { 
        // 获取连接
        Connection connection = RabbitmqConnectionUtil.getConnection(); 
        // 创建通道 
        Channel channel = connection.createChannel(); 
        // 声明队列 
        channel.queueDeclare(QUEUE_NAME, false, false, false, null); 
        //消息队列绑定到交换机 
        /*param1: 序列名 param2: 交换机名 param3: routingKey */ 
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "save"); 
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "update"); 
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "delete");
        // 定义队列 接收端==》消费者
        DefaultConsumer consumer = new DefaultConsumer(channel) { 
        // 监听队列中的消息，如果有消息，进行处理
        @Override 
        public void handleDelivery(String consumerTag, Envelope envelope, 
        AMQP.BasicProperties properties, 
        byte[] body) throws IOException { 
                // body： 消息中参数信息 
                String msg = new String(body); 
                System.out.println(" [消费者1模拟es服务] 接收到消息 : " + msg ); 
        }
}; 
/*param1 : 队列名称 param2 : 是否自动确认消息 param3 : 消费者 */

	channel.basicConsume(QUEUE_NAME, true, consumer); 
//消费者需要时时监听消息，不用关闭通道与连接 }
```

##### 消费者2

```
//交换机名称 
private final static String EXCHANGE_NAME = "direct_exchange_test"; 
//队列名称 
private final static String QUEUE_NAME = "direct_exchange_queue_2";
public static void main(String[] arg) throws Exception { 
        // 获取连接 
        Connection connection = RabbitmqConnectionUtil.getConnection(); 
        // 创建通道 
        Channel channel = connection.createChannel(); 
        // 声明队列 
        channel.queueDeclare(QUEUE_NAME, false, false, false, null); 
        //消息队列绑定到交换机 
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "save"); 
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "update"); 
        // 定义队列 接收端==》消费者
        DefaultConsumer consumer = new DefaultConsumer(channel) { 
        // 监听队列中的消息，如果有消息，进行处理 
        @Override 
        public void handleDelivery(String consumerTag, Envelope envelope, 
        AMQP.BasicProperties properties, 
        byte[] body) throws IOException { 
        // body： 消息中参数信息 
        String msg = new String(body); 
        System.out.println(" [消费者2模拟template服务] 接收到消息 : " + msg ); 
        } 
     }; 
        /*param1 : 队列名称 param2 : 是否自动确认消息 param3 : 消费者 */
        channel.basicConsume(QUEUE_NAME, true, consumer); 
        //消费者需要时时监听消息，不用关闭通道与连接 
}
```

执行完这三种routingKey后会发现消费者1能接受到三条消息,消费者2只能接收到两条消息

#### 订阅模型-Topic

Topic 类型的 Exchange 与 Direct 相比，都是可以根据 RoutingKey 把消息路由到不同的队列。只不

过 Topic 类型 Exchange 可以让队列在绑定 Routing key 的时候使用通配符！

Routingkey 一般都是有一个或多个单词组成，多个单词之间以”.”分割，例如： item.save

通配符规则：

 \# ：匹配一个或多个词

\* ：匹配不多不少恰好1个词

举例：

 audit.# ：能够匹配 audit.irs.corporate 或者 audit.irs 

 audit.* ：只能匹配 audit.irs

![](C:\Users\86156\Pictures\Camera Roll\屏幕截图 2021-03-11 215610.png)

##### 生产者

```
//交换机名称 
private final static String EXCHANGE_NAME = "topic_exchange_test"; 
//主函数 
public static void main(String[] arg) throws Exception { 
    // 获取到连接 
    Connection connection = RabbitmqConnectionUtil.getConnection(); 
    // 获取通道 
    Channel channel = connection.createChannel(); 
    /*param1: 交换机名称 param2: 交换机类型 */ 
    channel.exchangeDeclare(EXCHANGE_NAME,"topic");
    //发送的消息内容 
    String message = "商品删除成功 id ： 153"; 
    /*param1: 交换机名称 param2: routingKey param3: 一些配置信息 param4: 发送的消息 */ 
    //发送消息 
    .basicPublish(EXCHANGE_NAME, "goods.delete", null, message.getBytes()); //channel.basicPublish(EXCHANGE_NAME, "update", null, message.getBytes()); //channel.basicPublish(EXCHANGE_NAME, "delete", null, message.getBytes());
    System.out.println(" [商品服务] 发送消息routingKey ：delete '" + message ); 
    // 关闭通道和连接 
    channel.close();
    connection.close(); 
}
```

##### 消费者1

```
//交换机名称 
private final static String EXCHANGE_NAME = "topic_exchange_test"; 
//队列名称 
private final static String QUEUE_NAME = "topic_exchange_queue_1"; 
public static void main(String[] arg) throws Exception { 
        // 获取连接 
        Connection connection = RabbitmqConnectionUtil.getConnection(); 
        // 创建通道 
        Channel channel = connection.createChannel(); 
        // 声明队列 
        channel.queueDeclare(QUEUE_NAME, false, false, false, null); 
        消息队列绑定到交换机 
        /*param1: 序列名 param2: 交换机名 param3: routingKey */ 
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "goods.*"); 
        // 定义队列 接收端==》消费者 
        DefaultConsumer consumer = new DefaultConsumer(channel) { 
        	// 监听队列中的消息，如果有消息，进行处理 
            @Override 
            public void handleDelivery(String consumerTag, Envelope envelope, 
            		AMQP.BasicProperties properties, 
            				byte[] body) throws IOException { 
                        // body： 消息中参数信息
                        String msg = new String(body); 
                        System.out.println(" [消费者1模拟es服务] 接收到消息 : " + msg );
            } 
	};
        /*param1 : 队列名称 param2 : 是否自动确认消息 param3 : 消费者 */
        channel.basicConsume(QUEUE_NAME, true, consumer); 
        //消费者需要时时监听消息，不用关闭通道与连接
}
```

##### 消费者2

```
//交换机名称 
private final static String EXCHANGE_NAME = "topic_exchange_test"; 
//队列名称 
private final static String QUEUE_NAME = "topic_exchange_queue_2"; 
public static void main(String[] arg) throws Exception { 
        // 获取连接 
        Connection connection = RabbitmqConnectionUtil.getConnection(); 
        // 创建通道
        Channel channel = connection.createChannel(); 
        //声明队列 
        channel.queueDeclare(QUEUE_NAME, false, false, false, null); 
        //消息队列绑定到交换机 
        /*param1: 序列名 param2: 交换机名 param3: routingKey */ 
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "goods.update");
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "goods.save");
            // 定义队列 接收端==》消费者 
            DefaultConsumer consumer = new DefaultConsumer(channel) {
            // 监听队列中的消息，如果有消息，进行处理
                @Override 
                public void handleDelivery(String consumerTag, Envelope envelope, 
                	AMQP.BasicProperties properties,byte[] body) throws IOException { 
                        // body： 消息中参数信息 
                        String msg = new String(body); 
                        System.out.println(" [消费者2模拟es服务] 接收到消息 : " + msg ); 
            } 
      }; 
      /*param1 : 队列名称 param2 : 是否自动确认消息 param3 : 消费者 */
        channel.basicConsume(QUEUE_NAME, true, consumer); 
        //消费者需要时时监听消息，不用关闭通道与连接
}
```

####  RPc模型

##### 生产者

```
//交换机名称 
private final static String EXCHANGE_NAME = "exchange_rpc"; 
//队列名称 
private final static String QUEUE_NAME = "queue_rpc";
public static void main(String[] args)throws Exception { 
    // 获取连接 
    Connection connection = RabbitmqConnectionUtil.getConnection(); 
    // 创建通道 
    final Channel channel = connection.createChannel();
    //创建交换机--先删除后增加
    channel.exchangeDelete(EXCHANGE_NAME); 
    channel.exchangeDeclare(EXCHANGE_NAME, "direct", false, false, null); 
    //创建队列
    channel.queueDelete(QUEUE_NAME); 
    channel.queueDeclare(QUEUE_NAME, false, false, false, null); 
    //绑定队列交换机
    channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "rpc"); 
    //此处注意：我们声明了要回复的队列。队列名称由RabbitMQ自动创建。 
    //这样做的好处是：每个客户端有属于自己的唯一回复队列，生命周期同客户端 
    String replyQueue = channel.queueDeclare().getQueue(); 
    final String corrID ="9527"; 
    //消息 指定回复队列和ID 
    AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder(); 
    // 指定回复队列和回复
    correlateId builder.replyTo(replyQueue).correlationId(corrID); 
    AMQP.BasicProperties properties = builder.build(); 
    // 消息参数内容
    String message = "good good study ";
    System.out.println("rpc模型消息发送 "+message);
    channel.basicPublish(EXCHANGE_NAME, "rpc", properties, message.getBytes()); 
    DefaultConsumer consumer = new DefaultConsumer(channel) { 
        // 监听队列中的消息，如果有消息，进行处理 
        @Override 
        public void handleDelivery(String consumerTag, Envelope envelope, 
        AMQP.BasicProperties properties, byte[] body) throws IOException { 
            if (corrID.equals(properties.getCorrelationId())) {
            System.out.println("9527 ID对应上的消息：" + new String(body)); 
            } else { 
            System.out.println("9527 ID未对应上的消息：" + new String(body));
            } 
		} 
	};
		channel.basicConsume(replyQueue, true, consumer); 
} 
```

##### 消费者

```
//队列名称
private final static String QUEUE_NAME = "queue_rpc"; 
public static void main(String[] args) throws Exception { 
        //获取连接 
        Connection connection = RabbitmqConnectionUtil.getConnection(); 
        // 创建通道 
        final Channel channel = connection.createChannel(); 
        DefaultConsumer consumer = new DefaultConsumer(channel) { 
        // 监听队列中的消息，如果有消息，进行处理 
        @Override 
        public void handleDelivery(String consumerTag, Envelope envelope, 
        AMQP.BasicProperties properties, byte[] body) throws IOException { 
        System.out.println("收到消息 执行中："+new String(body)); 
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder(); 
        //我们在将要回复的消息属性中，放入从客户端传递过来的correlateId 关系id 
        builder.correlationId(properties.getCorrelationId()); 
        AMQP.BasicProperties prop = builder.build(); 
        //发送给回复队列的消息，exchange=""，routingKey=回复队列名称 
        //因为RabbitMQ对于队列，始终存在一个默认exchange=""，routingKey=队列名 称的绑定关系 
        String message= new String(body) + "-收到 over 一定 study"; 
        channel.basicPublish("", properties.getReplyTo(), prop,message.getBytes()); 
        } 
	};
	channel.basicConsume(QUEUE_NAME, true, consumer); 
}
```

####  确认模型

整个消息队列环境中都有三个角色

生产者

队列

消费者

基于这个问题我们来研究一下,消息有可能在那个环节丢失？？

\1. 生产者

说白了就是消息没有发送成功到队列中

\2. 队列

生产者成功发送消息到队列,在消费者还没有消费消息的时候重启队列(队列中的消息是存在内

存中的) 

\3. 消费者

消费者没有正常获取到数据

ok,三个环节都有可能发生丢失消息的情况

针对这三个环节我们可以用不同的方式解决消息丢失的问题

\1. 生产者消息丢失

使用确认模型处理

\2. 队列消息丢失

使用消息持久化

\3. 消费者消息丢失

\1. 手动ACK(这个我们已经学过了)

接下来我们说一下确认模型

##### 生产者

```
//交换机名称 
private final static String EXCHANGE_NAME = "exchange_pub";
public static void main(String[] args) throws Exception{ 
    Connection connection = RabbitmqConnectionUtil.getConnection(); 
    Channel channel = connection.createChannel(); 
    //是当前的channel处于确认模式 channel.confirmSelect(); 
    // 声明创建交换机exchange，指定类型为fanout 
    channel.exchangeDeclare(EXCHANGE_NAME, "fanout"); 
    //消息内容 
    String message="good good study"; 
    channel.basicPublish(EXCHANGE_NAME,"save",MessageProperties.PERSISTENT_BASIC,
    	message.getBytes()); 
        //确认消息，发送完毕 
        if(channel.waitForConfirms()){
        System.out.println("发送成功"); 
        }else{
        System.out.println("发送失败");
        }
    channel.close(); 
    connection.close(); 
}
```

#### 持久化

如何避免消息丢失？

1） 消费者的ACK机制。可以防止消费者丢失消息。

2） 但是，如果在消费者消费之前，MQ就宕机了，消息就没了。

是否可以将消息进行持久化呢？

要将消息持久化，前提是：队列、Exchange都持久化

![](C:\Users\86156\Pictures\Camera Roll\屏幕截图 2021-03-11 220124.png)

![](C:\Users\86156\Pictures\Camera Roll\屏幕截图 2021-03-11 220133.png)

## Spring AMQP

## 依赖和配置

### pom.xml

```
<dependency> 
    <groupId>org.springframework.boot</groupId> 
    <artifactId>spring-boot-starter-amqp</artifactId> 
</dependency>
```

###  application.yml

```
spring: 
	rabbitmq: 
        host: 127.0.0.1
        username: guest 
        password: guest 
        port: 5672
```

### 启动类

```
@SpringBootApplication 
public class RunTestRabbitMqApplication { 
	public static void main(String[] args) { 
    	SpringApplication.run(RunTestRabbitMqApplication.class); 
    	} 
    }
```

### 消费者

 在com.mr.rabbitmq包下新建spring 

在spring包下新建MqListener

```
@Component public class MqListener { @RabbitListener( bindings = @QueueBinding( value = @Queue(value = "topic_exchange_queue_1", durable = "true" ),exchange = @Exchange( value = "topic_exchange_test", ignoreDeclarationExceptions = "true", type = ExchangeTypes.TOPIC ),key = {"#.#"} ) )public void listen(String msg){ System.out.println("消费者接受到消息" + msg); } }
```

@RabbitListener 声明这个方法是一个消费者方法

bindings 指定绑定关系，可以有多个。值是 @QueueBinding 的数组

@QueueBinding 绑定队列

value 这个消费者关联的队列。值是 @Queue ，代表一个队列

value 消息队列名称

durable 是否持久化

exchange 队列所绑定的交换机，值是 @Exchange 类型

value 交换机名称

ignoreDeclarationExceptions 忽略声明异常

type 交换机类型

队列和交换机绑定的 RoutingKey

### 生产者

```
@RunWith(SpringRunner.class) 
@SpringBootTest(classes = RunTestRabbitMqApplication.class) 
public class SendMsg { 
    @Autowired 
    private AmqpTemplate amqpTemplate;

    @Test 
    public void sendMessage() throws InterruptedException { 
    String message = " good good study "; 
    //amqpTemplate 发送一个消息 指定：交换机名称， routingkey 参数
    amqpTemplate.convertAndSend("topic_exchange_test","x.x",message);
    System.out.println("发送成功：ok"); 
    // 等待10秒为了可以看到 消费者接收到消息执行 
    //Thread.sleep(10000); 
	} 
}
```

## 项目整合AMQP(rabbitmq)

##  mingrui-shop-service/pom.xml

```
<dependency> 
<groupId>org.springframework.boot</groupId> 
<artifactId>spring-boot-starter-amqp</artifactId> 
</dependency>
```

##  mingrui-shop-service-xxx

application.yml

```
spring: 
	rabbitmq: 
        host: 127.0.0.1 
        port: 5672 
        username: guest 
        password: guest 
        # 是否确认回调 
        publisher-confirm-type: correlated 
        # 是否返回回调 
        publisher-returns: true 
        virtual-host: / # 手动确认 
        listener: 
            simple: 
           		acknowledge-mode: manual
```

common-core项目新建com.baidu.shop.constant包

包下新建MqMessageConstant

```
public class MqMessageConstant { //spu交换机，routingkey public static final String SPU_ROUT_KEY_SAVE="spu.save"; public static final String SPU_ROUT_KEY_UPDATE="spu.update"; public static final String SPU_ROUT_KEY_DELETE="spu.delete"; //spu-es的队列 public static final String SPU_QUEUE_SEARCH_SAVE="spu_queue_es_save"; public static final String SPU_QUEUE_SEARCH_UPDATE="spu_queue_es_update"; public static final String SPU_QUEUE_SEARCH_DELETE="spu_queue_es_delete"; //spu-page的队列 public static final String SPU_QUEUE_PAGE_SAVE="spu_queue_page_save"; public static final String SPU_QUEUE_PAGE_UPDATE="spu_queue_page_update"; public static final String SPU_QUEUE_PAGE_DELETE="spu_queue_page_delete"; public static final String ALTERNATE_EXCHANGE = "exchange.ae"; public static final String EXCHANGE = "exchange.mr"; //Dead Letter Exchanges public static final String EXCHANGE_DLX = "exchange.dlx"; public static final String EXCHANGE_DLRK = "dlx.rk"; public static final Integer MESSAGE_TIME_OUT = 5000; public static final String QUEUE = "queue.mr"; public static final String QUEUE_AE = "queue.ae"; public static final String QUEUE_DLX = "queue.dlx"; public static final String ROUTING_KEY = "mrkey"; }
```

### 在com.baidu.shop下新建component包

### 在包下新建MrRabbitMQ

```
@Component @Slf4j public class MrRabbitMQ implements RabbitTemplate.ConfirmCallback, RabbitTemplate.ReturnCallback{private RabbitTemplate rabbitTemplate; //构造方法注入 @Autowired public MrRabbitMQ(RabbitTemplate rabbitTemplate) { this.rabbitTemplate = rabbitTemplate; //这是是设置回调能收到发送到响应 rabbitTemplate.setConfirmCallback(this); //如果设置备份队列则不起作用 rabbitTemplate.setMandatory(true); rabbitTemplate.setReturnCallback(this); }public void send(String sendMsg, String routingKey) { CorrelationData correlationId = new CorrelationData(UUID.randomUUID().toString()); //convertAndSend(exchange:交换机名称,routingKey:路由关键字,object:发送的消息 内容,correlationData:消息ID) rabbitTemplate.convertAndSend(MqMessageConstant.EXCHANGE, routingKey, sendMsg,correlationId); }@Override public void confirm(CorrelationData correlationData, boolean b, String s) { if(b){log.info("消息发送成 功:correlationData({}),ack({}),cause({})",correlationData,b,s); }else{log.error("消息发送失 败:correlationData({}),ack({}),cause({})",correlationData,b,s); } }@Override public void returnedMessage(Message message, int i, String s, String s1, String s2) { log.warn("消息丢 失:exchange({}),route({}),replyCode({}),replyText({}),message: {}",s1,s2,i,s,message); } }
```

### GoodsServiceImpl

```
@Transactional @Override public Result<JSONObject> saveGoods(SpuDTO spuDTO) { //新增spu SpuEntity spuEntity = BaiduBeanUtil.copyProperties(spuDTO, SpuEntity.class); spuEntity.setSaleable(1); spuEntity.setValid(1); final Date date = new Date();//保持两个时间一致 spuEntity.setCreateTime(date); spuEntity.setLastUpdateTime(date); spuMapper.insertSelective(spuEntity); //新增spuDetail SpuDetailEntity spuDetailEntity = BaiduBeanUtil.copyProperties(spuDTO.getSpuDetail(), SpuDetailEntity.class); spuDetailEntity.setSpuId(spuEntity.getId()); spuDetailMapper.insertSelective(spuDetailEntity); this.saveSkusAndStocks(spuDTO.getSkus(),spuEntity.getId(),date); mrRabbitMQ.send(spuEntity.getId() + "", MqMessageConstant.SPU_ROUT_KEY_SAVE); return this.setResultSuccess(); }
```

###  mingrui-shop-service-search

### application.yml

```
spring: 
	rabbitmq: 
        host: 127.0.0.1 
        port: 5672 
        username: guest 
        password: guest 
        # 是否确认回调 
        publisher-confirm-type: correlated 
        # 是否返回回调 
        publisher-returns: true
        virtual-host: / # 手动确认 
        listener:
        	simple: 
        		acknowledge-mode: manual
```

### ShopElasticsearchService

```
@ApiOperation(value = "新增数据到es") 

@PostMapping(value = "es/saveData") 

Result<JSONObject> saveData(Integer spuId); 



@ApiOperation(value = "通过id删除es数据") 

@DeleteMapping(value = "es/saveData") 

Result<JSONObject> delData(Integer spuId);
```



### ShopElasticsearchServiceImpl

```
@Override 

public Result<JSONObject> saveData(Integer spuId) { 

SpuDTO spuDTO = new SpuDTO(); 

spuDTO.setId(spuId); 

List<GoodsDoc> goodsDocs = this.esGoodsInfo(spuDTO); 

GoodsDoc goodsDoc = goodsDocs.get(0); 

elasticsearchRestTemplate.save(goodsDoc); 

return this.setResultSuccess(); 

}
```

```
@Override 

public Result<JSONObject> delData(Integer spuId) { 

GoodsDoc goodsDoc = new GoodsDoc(); 

goodsDoc.setId(spuId.longValue()); 

elasticsearchRestTemplate.delete(goodsDoc); 

return this.setResultSuccess(); 

} 
```

### com.baidu.shop下新建包listener

###  包下新建GoodsListener

```
import com.baidu.shop.constant.MqMessageConstant; 

import com.baidu.shop.service.ShopElasticsearchService; 

import com.rabbitmq.client.Channel; 

import lombok.extern.slf4j.Slf4j; 

import org.springframework.amqp.core.ExchangeTypes; 

import org.springframework.amqp.core.Message; 

import org.springframework.amqp.rabbit.annotation.Exchange; 

import org.springframework.amqp.rabbit.annotation.Queue; 

import org.springframework.amqp.rabbit.annotation.QueueBinding; 

import org.springframework.amqp.rabbit.annotation.RabbitListener; 

import org.springframework.beans.factory.annotation.Autowired; 

import org.springframework.stereotype.Component; 

import java.io.IOException; 

/**

\* @ClassName GoodsListener 

\* @Description: TODO 

\* @Author shenyaqi 

\* @Date 2020/9/17 

\* @Version V1.0 

**/ 

@Component 

@Slf4j 

public class GoodsListener { 

@Autowired 

private ShopElasticsearchService shopElasticsearchService; 

@RabbitListener( 

bindings = @QueueBinding( 

value = @Queue( 

value = MqMessageConstant.SPU_QUEUE_SEARCH_SAVE

durable = "true" 

),

exchange = @Exchange( 

value = MqMessageConstant.EXCHANGE, 

ignoreDeclarationExceptions = "true", 

type = ExchangeTypes.TOPIC 

),

key = 

{MqMessageConstant.SPU_ROUT_KEY_SAVE,MqMessageConstant.SPU_ROUT_KEY_UPDATE} 

) 

)

public void save(Message message, Channel channel) throws IOException { 

log.info("es服务接受到需要保存数据的消息: " + new String(message.getBody())); 

//新增数据到es 

shopElasticsearchService.saveData(Integer.parseInt(new 

String(message.getBody()))); 

channel.basicAck(message.getMessageProperties().getDeliveryTag(), true); 

}

@RabbitListener( 

bindings = @QueueBinding( 

value = @Queue( 

value = MqMessageConstant.SPU_QUEUE_SEARCH_DELETE, 

durable = "true" 

),

exchange = @Exchange( 

value = MqMessageConstant.EXCHANGE, 

ignoreDeclarationExceptions = "true", 

type = ExchangeTypes.TOPIC 

),

key = MqMessageConstant.SPU_ROUT_KEY_DELETE 

) 

)

public void delete(Message message, Channel channel) throws IOException { 

log.info("es服务接受到需要删除数据的消息: " + new String(message.getBody())); 

//新增数据到es 

shopElasticsearchService.delData(Integer.parseInt(new 

String(message.getBody()))); 

channel.basicAck(message.getMessageProperties().getDeliveryTag(), true); 

} 

}
```

## mingrui-shop-service-template

### application.yml

```
spring: 

rabbitmq: 

host: 127.0.0.1 

port: 5672 

username: guest 

password: guest 

\# 是否确认回调 

publisher-confirm-type: correlated 

\# 是否返回回调 

publisher-returns: true 

virtual-host: / 

\# 手动确认 

listener: 

simple: 

acknowledge-mode: manual 
```

### TemplateService

```
@DeleteMapping(value = "template/delHTMLBySpuId") 

Result<JSONObject> delHTMLBySpuId(Integer spuId); 
```

### TemplateServiceImpl

```
@Override 

public Result<JSONObject> delHTMLBySpuId(Integer spuId) { 

File file = new File(staticHTMLPath + File.separator + spuId + ".html"); 

if(!file.delete()){ 

return this.setResultError("文件删除失败"); 

}

return this.setResultSuccess(); 

} 
```

### com.baidu.shop包下新建istener包

### 包下新建emplateListener

```
import com.baidu.shop.constant.MqMessageConstant; 

import com.baidu.shop.service.TemplateService; 

import com.rabbitmq.client.Channel; 

import lombok.extern.slf4j.Slf4j; 

import org.springframework.amqp.core.ExchangeTypes; 

import org.springframework.amqp.core.Message; 

import org.springframework.amqp.rabbit.annotation.Exchange; 

import org.springframework.amqp.rabbit.annotation.Queue; 

import org.springframework.amqp.rabbit.annotation.QueueBinding; 

import org.springframework.amqp.rabbit.annotation.RabbitListener; 

import org.springframework.beans.factory.annotation.Autowired; 

import org.springframework.stereotype.Component;

import java.io.IOException; 

/**

\* @ClassName TemplateListener 

\* @Description: TODO 

\* @Author shenyaqi 

\* @Date 2020/9/18 

\* @Version V1.0 

**/ 

@Component 

@Slf4j 

public class TemplateListener { 

@Autowired 

private TemplateService templateService; 

@RabbitListener( 

bindings = @QueueBinding( 

value = @Queue( 

value = MqMessageConstant.SPU_QUEUE_PAGE_SAVE, 

durable = "true" 

),

exchange = @Exchange( 

value = MqMessageConstant.EXCHANGE, 

ignoreDeclarationExceptions = "true", 

type = ExchangeTypes.TOPIC 

),

key = 

{MqMessageConstant.SPU_ROUT_KEY_SAVE,MqMessageConstant.SPU_ROUT_KEY_UPDATE} 

) 

)

public void save(Message message, Channel channel) throws IOException { 

log.info("template服务接受到需要保存数据的消息: " + new 

String(message.getBody())); 

//根据spuId生成页面 

templateService.createStaticHTMLTemplate(Integer.valueOf(new 

String(message.getBody()))); 

channel.basicAck(message.getMessageProperties().getDeliveryTag(), true); 

}

@RabbitListener( 

bindings = @QueueBinding( 

value = @Queue( 

value = MqMessageConstant.SPU_QUEUE_PAGE_DELETE, 

durable = "true" 

),

exchange = @Exchange( 

value = MqMessageConstant.EXCHANGE, 

ignoreDeclarationExceptions = "true", 

type = ExchangeTypes.TOPIC 

),

key = MqMessageConstant.SPU_ROUT_KEY_DELETE 

) 

)

public void delete(Message message, Channel channel) throws IOException {

log.info("template服务接受到需要删除数据的消息: " + new 

String(message.getBody())); 

//根据spuid删除页面 

templateService.delHTMLBySpuId(Integer.valueOf(new 

String(message.getBody()))); 

channel.basicAck(message.getMessageProperties().getDeliveryTag(), true); 

} 
```

# 第一次测试

![](C:\Users\86156\Pictures\Camera Roll\屏幕截图 2021-03-11 221731.png)

![](C:\Users\86156\Pictures\Camera Roll\屏幕截图 2021-03-11 221747.png)

![](C:\Users\86156\Pictures\Camera Roll\屏幕截图 2021-03-11 221759.png)

##  测试流程

![](C:\Users\86156\Pictures\Camera Roll\屏幕截图 2021-03-11 221853.png)

![](C:\Users\86156\Pictures\Camera Roll\屏幕截图 2021-03-11 221905.png)

![](C:\Users\86156\Pictures\Camera Roll\屏幕截图 2021-03-11 221919.png)

![](C:\Users\86156\Pictures\Camera Roll\屏幕截图 2021-03-11 221930.png)

可以发现确实是没有查询到数据

这是为啥呢

我们再从生产者发消息那块找一下看能不能发现什么信息

![](C:\Users\86156\Pictures\Camera Roll\屏幕截图 2021-03-11 222020.png)

![](C:\Users\86156\Pictures\Camera Roll\屏幕截图 2021-03-11 222045.png)

ok现在问题已经很明了了

现在的问题是 我新增正常执行了,但是到方法结束之前还没有进行提交事务(这个时候大家就得去复习一

下spring 的事务了)

我们现在用到了@Transactional这个注解,这个注解的作用就是帮我们提交事务

我方法没有执行完成的时候是不会提交事务的

也就是是说栈针出栈的时候才会提交事务,

那我们知道了,应该是我消息已经发送成功了,消费者也接受到消息了,但是你的事务还没有提交,所以在消

费端查询不到数据!!!!

知道问题出现的原因以后,我们就可以解决这个问题了

