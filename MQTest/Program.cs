using Dal;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace MQTest
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
            var channel = connection.CreateModel();//声明一个队列
            channel.QueueDeclare("cache", false, false, false, null);
            Console.WriteLine("RabbitMQ连接成功");
            string str;
            do
            {
                str = Console.ReadLine();
                var sendBytes = Encoding.UTF8.GetBytes(str);
                //发布消息    
                channel.BasicPublish("", "cache", null, sendBytes);
            } while (str!="exit");
            channel.Close();
            connection.Close();
        }
    }
}
