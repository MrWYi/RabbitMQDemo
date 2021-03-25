using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace MongoLib
{
    public class MongoDBHelper
    {
        public static string DBName = "LimsGeno";
        public static string mongoUrl = "mongodb://192.168.2.54:27017";
        #region
        public void MongoTestDemo()
        {
            //建立连接

            IMongoDatabase database = createMongoConnection(mongoUrl, DBName);

            #region 添加
            //建立collection
            var collection = database.GetCollection<BsonDocument>("testMongo");

            var document = new BsonDocument
            {
                {"name","MongoDB"},
                {"type","Database"},
                {"count",1},
                {"info",new BsonDocument{{"x",203},{"y",102}}}
            };
            //插入数据
            collection.InsertOne(document);

            var count = collection.Count(document);
            Console.WriteLine("Add:" + count);

            var document2 = new BsonDocument
            {
                {"name","MongoDB2"},
                {"type","Database"},
                {"count",1},
                {"info",new BsonDocument{{"x",203},{"y",102}}}
            };
            //插入数据
            collection.InsertOne(document2);

            count = collection.Count(document2);
            Console.WriteLine("Add:" + count);
            #endregion

            #region 查询
            var filter = Builders<BsonDocument>.Filter.Eq("name", "MongoDB");
            //查询数据
            var document1 = collection.Find(filter);
            Console.WriteLine(document1.ToString());
            #endregion

            #region 更新

            var filter3 = Builders<BsonDocument>.Filter.Eq("name", "Ghazi3");
            var document3 = new BsonDocument
            {
                {"name","MongoD1B"},
                {"type","Database1"},
                {"count",2},
                {"info",new BsonDocument{{"x",203},{"y",102}}},
                { "replace",1}
            };
            
            collection.ReplaceOne(filter3, document3);
            Console.WriteLine("ReplaceOne:{0}", "MongoD1B");
            #endregion

            #region 删除
            //删除数据
            var filter1 = Builders<BsonDocument>.Filter.Eq("name", "Ghazi");
            collection.DeleteMany(filter1);
            Console.WriteLine("Deleted:{0}", "Ghazi");
            #endregion


            var filterbb = Builders<BsonDocument>.Filter.Eq("count", 1);

            var result = collection.Find(filterbb);
            var ss = result.ToList<BsonDocument>();

            foreach (var item in ss)
            {
                Console.WriteLine(item.ToString());
            }

            

            //BsonDocument document2 = new BsonDocument();
            //document2.Add("name", "MongoDB");
            //document2.Add("type", "Database");
            //document2.Add("count", "1");

            //collection.InsertOne(document2);

            
        }

        /// <summary>
        ///  建立数据库
        /// </summary>
        /// <param name="url"></param>
        /// <param name="dbName"></param>
        /// <returns></returns>
        public static IMongoDatabase createMongoConnection(string url,string dbName)
        {
            MongoUrl mongoUrl = new MongoUrl(url);
            var settings = MongoClientSettings.FromUrl(mongoUrl);

            var client = new MongoClient(settings);
            var database = client.GetDatabase(dbName);
            return database;
        }
        #endregion

        #region

        public static void Insert(BsonDocument doc,string dbName,string tbName)
        {
            //建立连接
            var client = new MongoClient();
            //建立数据库
            var database = client.GetDatabase(dbName);

            var collection = database.GetCollection<BsonDocument>(tbName);

            collection.InsertOne(doc);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dbName"></param>
        /// <param name="tbName"></param>
        /// <param name="filter"></param>
        /// <param name="update"></param>
        /// <returns></returns>
        public UpdateResult Update(string dbName, string tbName,FilterDefinition<BsonDocument> filter,UpdateDefinition<BsonDocument> update)
        {
            //建立连接
            var client = new MongoClient();
            //建立数据库
            var database = client.GetDatabase(dbName);

            var collection = database.GetCollection<BsonDocument>(tbName);

            //Builders<BsonDocument>.Filter.AnyEq()
            //var filter = Builders<BsonDocument>.Filter.Eq("name", "MongoDB");
            //var update = Builders<BsonDocument>.Update.Set("name", "Ghazi");

            UpdateResult result = collection.UpdateOne(filter, update);

           
            return result;
        }


        public ReplaceOneResult Replace(string dbName, string tbName, FilterDefinition<BsonDocument> filter, BsonDocument bson)
        {
            //建立连接
            var client = new MongoClient();
            //建立数据库
            var database = client.GetDatabase(dbName);

            var collection = database.GetCollection<BsonDocument>(tbName);
          

            var result = collection.ReplaceOne(filter, bson);
            return result;
        }
        public DeleteResult Delete(string dbName, string tbName, FilterDefinition<BsonDocument> filter)
        {
            //建立连接
            var client = new MongoClient();
            //建立数据库
            var database = client.GetDatabase(dbName);

            var collection = database.GetCollection<BsonDocument>(tbName);

            DeleteResult result= collection.DeleteMany(filter);

            return result;
        }

        #endregion
    }
}
