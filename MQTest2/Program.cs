using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace MQTest2
{
    class Program
    {
        static void Main(string[] args)
        {
            //创建连接工厂            
            ConnectionFactory factory = new ConnectionFactory
            {
                UserName = "syncdata",//用户名                
                Password = "syncdata",//密码                
                HostName = "192.168.1.20"//rabbitmq ip            
            };
            //创建连接            
            var connection = factory.CreateConnection();
            //创建通道            
            var channel = connection.CreateModel();
            //事件基本消费者          
            EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
            //接收到消息事件          
            consumer.Received += (ch, ea) =>
            {
                var b = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(b);
                Console.WriteLine($"收到消息： {message}");
                //确认该消息已被消费                
                channel.BasicAck(ea.DeliveryTag, false);
            };            
            //启动消费者 设置为手动应答消息            
            channel.BasicConsume("cache", false, consumer); 
            Console.WriteLine("消费者已启动"); 
            Console.ReadKey();
            channel.Dispose();
            connection.Close();
        }
    }
}
