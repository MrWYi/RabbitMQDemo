using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Driver;
using MongoDB.Bson;

namespace RX.DNA.Match.MongoDB.Helper
{
    /// <summary>
    /// mongodb的封装类。
    /// add by yancong2008@gmail.com 2011.05.14
    /// </summary>
    public sealed class MongoDBHelper2
    //public sealed class MongoDBHelper<T>
    //where T :class
    {
        /*
        //mongodb://[username:password@]host1[:port1][,host2[:port2],…[,hostN[:portN]]][/[database][?options]]
        public static string connectionString_Default = "mongodb://test:test@127.0.0.1:27017"; //System.Configuration.ConfigurationManager.AppSettings["ConnectionString_mongoDB"];
        public static string database_Default = "Test"; //System.Configuration.ConfigurationManager.AppSettings["Database_mongoDB"];

        #region 创建连接

        

        #endregion

        #region 新增
        public static WriteConcernResult InsertOne<T>(string collectionName, T entity)
        {
           
            return MongoDBHelper2.InsertOne<T>(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName, entity);
        }

        /*
        public static WriteConcernResult InsertOne<T>(string connectionString, string databaseName, string collectionName, T entity)
        {
            //WriteConcernResult result = new WriteConcernResult();
            if (null == entity)
            {
                return null;
            }
            //MongoServer server = new MongoClient(connectionString).GetServer();
            //MongoServer server = MongoServer.Create(connectionString);
            MongoClient mc = new MongoClient(connectionString);
            //获取数据库或者创建数据库（不存在的话）。
            IMongoDatabase database = mc.GetDatabase(databaseName);
            //using (server.RequestStart(database))//开始连接数据库。
            //{
                IMongoCollection<BsonDocument> myCollection = database.GetCollection<BsonDocument>(collectionName);
                myCollection.InsertOne((BsonDocument)entity);
                //WriteConcernResult result = myCollection.Insert(entity);
            //}
            return result;
        }
        
        public static void InsertOne<T>(string connectionString, string databaseName, string collectionName, BsonDocument entity)
        {
            //WriteConcernResult result = new WriteConcernResult();
            if (null == entity)
            {
                //return null;
            }
            MongoClient mc = new MongoClient(connectionString);
            //获取数据库或者创建数据库（不存在的话）。
            IMongoDatabase database = mc.GetDatabase(databaseName);
            IMongoCollection<BsonDocument> myCollection = database.GetCollection<BsonDocument>(collectionName);
            myCollection.InsertOne((BsonDocument)entity);
        }


        //public static IEnumerable<WriteConcernResult> InsertAll<T>(string collectionName, IEnumerable<BsonDocument> entitys)
        //{
        //    return MongoDBHelper.InsertAll<T>(MongoDBHelper.connectionString_Default, MongoDBHelper.database_Default, collectionName, entitys);
        //}
        public static void InsertAll<T>(string connectionString, string databaseName, string collectionName, IEnumerable<BsonDocument> entitys)
        {
            IEnumerable<WriteConcernResult> result = null;
            if (null == entitys)
            {
                //return null;
            }
            MongoClient mc = new MongoClient(connectionString);
            //获取数据库或者创建数据库（不存在的话）。
            IMongoDatabase database = mc.GetDatabase(databaseName);
            IMongoCollection<BsonDocument> myCollection = database.GetCollection<BsonDocument>(collectionName);
            myCollection.InsertMany(entitys);
           
        }
        #endregion

        #region 修改


        public static void UpdateOne<T>(string collectionName, BsonDocument entity)
        {
            return MongoDBHelper2.UpdateOne<T>(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName, entity);
        }

        public static WriteConcernResult UpdateOne<T>(string database_Default, string collectionName, BsonDocument entity)
        {
            return MongoDBHelper2.UpdateOne<T>(MongoDBHelper2.connectionString_Default, database_Default, collectionName, entity);
        }

        public static void UpdateOne<T>(string connectionString, string databaseName, string collectionName, BsonDocument entity)
        {
            WriteConcernResult result;
   
            //MongoServer server = new MongoClient(connectionString).GetServer();
            //获取数据库或者创建数据库（不存在的话）。
           // MongoDatabase database = server.GetDatabase(databaseName);
            //using (server.RequestStart(database))//开始连接数据库。
            //{
            //    MongoCollection<BsonDocument> myCollection = database.GetCollection<BsonDocument>(collectionName);
            //    result = myCollection.Save(entity);
            //}
            IMongoDatabase database= new MongoClient(connectionString).GetDatabase(databaseName);
            //using(database.wr)
            IMongoCollection<BsonDocument> imc= database.GetCollection<BsonDocument>(collectionName);
            imc.UpdateOne()
            return result;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="query">条件查询。 调用示例：Query.Matches("Title", "感冒") 或者 Query.EQ("Title", "感冒") 或者Query.And(Query.Matches("Title", "感冒"),Query.EQ("Author", "yanc")) 等等</param>
        /// <param name="update">更新设置。调用示例：Update.Set("Title", "yanc") 或者 Update.Set("Title", "yanc").Set("Author", "yanc2") 等等</param>
        /// <returns></returns>
        public static WriteConcernResult UpdateAll<T>(string collectionName, IMongoQuery query, IMongoUpdate update)
        {
            return MongoDBHelper2.UpdateAll<T>(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName, query, update);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connectionString"></param>
        /// <param name="databaseName"></param>
        /// <param name="collectionName"></param>
        /// <param name="query">条件查询。 调用示例：Query.Matches("Title", "感冒") 或者 Query.EQ("Title", "感冒") 或者Query.And(Query.Matches("Title", "感冒"),Query.EQ("Author", "yanc")) 等等</param>
        /// <param name="update">更新设置。调用示例：Update.Set("Title", "yanc") 或者 Update.Set("Title", "yanc").Set("Author", "yanc2") 等等</param>
        /// <returns></returns>
        public static WriteConcernResult UpdateAll<T>(string connectionString, string databaseName, string collectionName, IMongoQuery query, IMongoUpdate update)
        {
            WriteConcernResult result;
            if (null == query || null == update)
            {

                return null;
            }
            MongoClient mc = new MongoClient(connectionString);
            //获取数据库或者创建数据库（不存在的话）。
            IMongoDatabase database = mc.GetDatabase(databaseName);
            using (server.RequestStart(database))//开始连接数据库。
            {
                IMongoCollection<BsonDocument> myCollection = database.GetCollection<BsonDocument>(collectionName);
                result = myCollection.UpdateMany(query,up)
                    (query, update, UpdateFlags.Multi);
            }
            return result;
        }
        #endregion

        #region 删除
        public static WriteConcernResult Delete(string collectionName, string _id)
        {
            return MongoDBHelper2.Delete(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName, _id);
        }
        public static WriteConcernResult Delete(string connectionString, string databaseName, string collectionName, string _id)
        {
            WriteConcernResult result;
            ////ObjectId id;
            ////if (!ObjectId.TryParse(_id, out id))
            ////{
            ////    return null;
            ////}
            MongoServer server = new MongoClient(connectionString).GetServer();
            //获取数据库或者创建数据库（不存在的话）。
            MongoDatabase database = server.GetDatabase(databaseName);
            using (server.RequestStart(database))//开始连接数据库。
            {
                MongoCollection<BsonDocument> myCollection = database.GetCollection<BsonDocument>(collectionName);
                result = myCollection.Remove(Query.EQ("_id", _id));
            }
            return result;
        }
        public static WriteConcernResult DeleteAll(string collectionName)
        {
            return MongoDBHelper2.DeleteAll(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName, null);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="collectionName"></param>
        /// <param name="query">条件查询。 调用示例：Query.Matches("Title", "感冒") 或者 Query.EQ("Title", "感冒") 或者Query.And(Query.Matches("Title", "感冒"),Query.EQ("Author", "yanc")) 等等</param>
        /// <returns></returns>
        public static WriteConcernResult DeleteAll(string collectionName, IMongoQuery query)
        {
            return MongoDBHelper2.DeleteAll(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName, query);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="databaseName"></param>
        /// <param name="collectionName"></param>
        /// <param name="query">条件查询。 调用示例：Query.Matches("Title", "感冒") 或者 Query.EQ("Title", "感冒") 或者Query.And(Query.Matches("Title", "感冒"),Query.EQ("Author", "yanc")) 等等</param>
        /// <returns></returns>
        public static WriteConcernResult DeleteAll(string connectionString, string databaseName, string collectionName, IMongoQuery query)
        {
            MongoServer server = new MongoClient(connectionString).GetServer();
            //获取数据库或者创建数据库（不存在的话）。
            MongoDatabase database = server.GetDatabase(databaseName);
            WriteConcernResult result;
            using (server.RequestStart(database))//开始连接数据库。
            {
                MongoCollection<BsonDocument> myCollection = database.GetCollection<BsonDocument>(collectionName);
                if (null == query)
                {
                    result = myCollection.RemoveAll();
                }
                else
                {
                    result = myCollection.Remove(query);
                }
            }
            return result;
        }
        #endregion

        #region 获取单条信息
        public static T GetOne<T>(string collectionName, string _id)
        {
            return MongoDBHelper2.GetOne<T>(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName, _id);
        }
        public static T GetOne<T>(string connectionString, string databaseName, string collectionName, string _id)
        {
            T result = default(T);
            ObjectId id;
            if (!ObjectId.TryParse(_id, out id))
            {
                return default(T);
            }
            MongoServer server = new MongoClient(connectionString).GetServer();
            //获取数据库或者创建数据库（不存在的话）。
            MongoDatabase database = server.GetDatabase(databaseName);
            using (server.RequestStart(database))//开始连接数据库。
            {
                MongoCollection<BsonDocument> myCollection = database.GetCollection<BsonDocument>(collectionName);
                result = myCollection.FindOneAs<T>(Query.EQ("_id", id));
            }
            return result;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="query">条件查询。 调用示例：Query.Matches("Title", "感冒") 或者 Query.EQ("Title", "感冒") 或者Query.And(Query.Matches("Title", "感冒"),Query.EQ("Author", "yanc")) 等等</param>
        /// <returns></returns>
        public static T GetOne<T>(string collectionName, IMongoQuery query)
        {
            return GetOne<T>(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName, query);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connectionString"></param>
        /// <param name="databaseName"></param>
        /// <param name="collectionName"></param>
        /// <param name="query">条件查询。 调用示例：Query.Matches("Title", "感冒") 或者 Query.EQ("Title", "感冒") 或者Query.And(Query.Matches("Title", "感冒"),Query.EQ("Author", "yanc")) 等等</param>
        /// <returns></returns>
        public static T GetOne<T>(string connectionString, string databaseName, string collectionName, IMongoQuery query)
        {
            MongoServer server = new MongoClient(connectionString).GetServer();
            //获取数据库或者创建数据库（不存在的话）。
            MongoDatabase database = server.GetDatabase(databaseName);
            T result = default(T);
            using (server.RequestStart(database))//开始连接数据库。
            {
                MongoCollection<BsonDocument> myCollection = database.GetCollection<BsonDocument>(collectionName);
                if (null == query)
                {
                    result = myCollection.FindOneAs<T>();
                }
                else
                {
                    result = myCollection.FindOneAs<T>(query);
                }
            }
            return result;
        }
        #endregion

        #region 获取多个
        public static List<T> GetAll<T>(string collectionName)
        {
            return MongoDBHelper2.GetAll<T>(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName);
        }
        /// <summary>
        /// 如果不清楚具体的数量，一般不要用这个函数。
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <returns></returns>
        public static List<T> GetAll<T>(string connectionString, string databaseName, string collectionName)
        {
            MongoServer server = new MongoClient(connectionString).GetServer();
            //获取数据库或者创建数据库（不存在的话）。
            MongoDatabase database = server.GetDatabase(databaseName);
            List<T> result = new List<T>();
            using (server.RequestStart(database))//开始连接数据库。
            {
                MongoCollection<BsonDocument> myCollection = database.GetCollection<BsonDocument>(collectionName);
                foreach (T entity in myCollection.FindAllAs<T>())
                {
                    result.Add(entity);
                }
            }
            return result;
        }
        public static List<T> GetAll<T>(string collectionName, int count)
        {
            return MongoDBHelper2.GetAll<T>(collectionName, count, null, null);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="count"></param>
        /// <param name="query">条件查询。 调用示例：Query.Matches("Title", "感冒") 或者 Query.EQ("Title", "感冒") 或者Query.And(Query.Matches("Title", "感冒"),Query.EQ("Author", "yanc")) 等等</param>
        /// <returns></returns>
        public static List<T> GetAll<T>(string collectionName, int count, IMongoQuery query)
        {
            return MongoDBHelper2.GetAll<T>(collectionName, count, query, null);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="count"></param>
        /// <param name="sortBy">排序用的。调用示例：SortBy.Descending("Title") 或者 SortBy.Descending("Title").Ascending("Author")等等</param>
        /// <returns></returns>
        public static List<T> GetAll<T>(string collectionName, int count, IMongoSortBy sortBy)
        {
            return MongoDBHelper2.GetAll<T>(collectionName, count, null, sortBy);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="count"></param>
        /// <param name="query">条件查询。 调用示例：Query.Matches("Title", "感冒") 或者 Query.EQ("Title", "感冒") 或者Query.And(Query.Matches("Title", "感冒"),Query.EQ("Author", "yanc")) 等等</param>
        /// <param name="sortBy">排序用的。调用示例：SortBy.Descending("Title") 或者 SortBy.Descending("Title").Ascending("Author")等等</param>
        /// <param name="fields">只返回所需要的字段的数据。调用示例："Title" 或者 new string[] { "Title", "Author" }等等</param>
        /// <returns></returns>
        public static List<T> GetAll<T>(string collectionName, int count, IMongoQuery query, IMongoSortBy sortBy, params string[] fields)
        {
            PagerInfo pagerInfo = new PagerInfo();
            pagerInfo.Page = 1;
            pagerInfo.PageSize = count;
            return MongoDBHelper2.GetAll<T>(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName, query, pagerInfo, sortBy, fields);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="query">条件查询。 调用示例：Query.Matches("Title", "感冒") 或者 Query.EQ("Title", "感冒") 或者Query.And(Query.Matches("Title", "感冒"),Query.EQ("Author", "yanc")) 等等</param>
        /// <param name="pagerInfo"></param>
        /// <returns></returns>
        public static List<T> GetAll<T>(string collectionName, IMongoQuery query, PagerInfo pagerInfo)
        {
            return MongoDBHelper2.GetAll<T>(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName, query, pagerInfo, null);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="query">条件查询。 调用示例：Query.Matches("Title", "感冒") 或者 Query.EQ("Title", "感冒") 或者Query.And(Query.Matches("Title", "感冒"),Query.EQ("Author", "yanc")) 等等</param>
        /// <param name="pagerInfo"></param>
        /// <param name="sortBy">排序用的。调用示例：SortBy.Descending("Title") 或者 SortBy.Descending("Title").Ascending("Author")等等</param>
        /// <returns></returns>
        public static List<T> GetAll<T>(string collectionName, IMongoQuery query, PagerInfo pagerInfo, IMongoSortBy sortBy)
        {
            return MongoDBHelper2.GetAll<T>(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName, query, pagerInfo, sortBy);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="query">条件查询。 调用示例：Query.Matches("Title", "感冒") 或者 Query.EQ("Title", "感冒") 或者Query.And(Query.Matches("Title", "感冒"),Query.EQ("Author", "yanc")) 等等</param>
        /// <param name="pagerInfo"></param>
        /// <param name="fields">只返回所需要的字段的数据。调用示例："Title" 或者 new string[] { "Title", "Author" }等等</param>
        /// <returns></returns>
        public static List<T> GetAll<T>(string collectionName, IMongoQuery query, PagerInfo pagerInfo, params string[] fields)
        {
            return MongoDBHelper2.GetAll<T>(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName, query, pagerInfo, null, fields);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="query">条件查询。 调用示例：Query.Matches("Title", "感冒") 或者 Query.EQ("Title", "感冒") 或者Query.And(Query.Matches("Title", "感冒"),Query.EQ("Author", "yanc")) 等等</param>
        /// <param name="pagerInfo"></param>
        /// <param name="sortBy">排序用的。调用示例：SortBy.Descending("Title") 或者 SortBy.Descending("Title").Ascending("Author")等等</param>
        /// <param name="fields">只返回所需要的字段的数据。调用示例："Title" 或者 new string[] { "Title", "Author" }等等</param>
        /// <returns></returns>
        public static List<T> GetAll<T>(string collectionName, IMongoQuery query, PagerInfo pagerInfo, IMongoSortBy sortBy, params string[] fields)
        {
            return MongoDBHelper2.GetAll<T>(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName, query, pagerInfo, sortBy, fields);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connectionString"></param>
        /// <param name="databaseName"></param>
        /// <param name="collectionName"></param>
        /// <param name="query">条件查询。 调用示例：Query.Matches("Title", "感冒") 或者 Query.EQ("Title", "感冒") 或者Query.And(Query.Matches("Title", "感冒"),Query.EQ("Author", "yanc")) 等等</param>
        /// <param name="pagerInfo"></param>
        /// <param name="sortBy">排序用的。调用示例：SortBy.Descending("Title") 或者 SortBy.Descending("Title").Ascending("Author")等等</param>
        /// <param name="fields">只返回所需要的字段的数据。调用示例："Title" 或者 new string[] { "Title", "Author" }等等</param>
        /// <returns></returns>
        public static List<T> GetAll<T>(string connectionString, string databaseName, string collectionName, IMongoQuery query, PagerInfo pagerInfo, IMongoSortBy sortBy, params string[] fields)
        {
            MongoServer server = new MongoClient(connectionString).GetServer();
            //获取数据库或者创建数据库（不存在的话）。
            MongoDatabase database = server.GetDatabase(databaseName);
            List<T> result = new List<T>();
            using (server.RequestStart(database))//开始连接数据库。
            {
                MongoCollection<BsonDocument> myCollection = database.GetCollection<BsonDocument>(collectionName);
                MongoCursor<T> myCursor;
                if (null == query)
                {
                    myCursor = myCollection.FindAllAs<T>();
                }
                else
                {
                    myCursor = myCollection.FindAs<T>(query);
                }
                if (null != sortBy)
                {
                    myCursor.SetSortOrder(sortBy);
                }
                if (null != fields)
                {
                    myCursor.SetFields(fields);
                }
                foreach (T entity in myCursor.SetSkip((pagerInfo.Page - 1) * pagerInfo.PageSize).SetLimit(pagerInfo.PageSize))//.SetSkip(100).SetLimit(10)是指读取第一百条后的10条数据。
                {
                    result.Add(entity);
                }
            }
            return result;
        }
        #endregion

        #region 游标
        

        #endregion

        #region 索引
        public static void CreateIndex(string collectionName, params string[] keyNames)
        {
            MongoDBHelper2.CreateIndex(MongoDBHelper2.connectionString_Default, MongoDBHelper2.database_Default, collectionName, keyNames);
        }
        public static void CreateIndex(string connectionString, string databaseName, string collectionName, params string[] keyNames)
        {
            WriteConcernResult result = new WriteConcernResult();
            if (null == keyNames)
            {
                return;
            }
            MongoServer server = new MongoClient(connectionString).GetServer();
            //获取数据库或者创建数据库（不存在的话）。
            MongoDatabase database = server.GetDatabase(databaseName);
            using (server.RequestStart(database))//开始连接数据库。
            {
                MongoCollection<BsonDocument> myCollection = database.GetCollection<BsonDocument>(collectionName);
                if (!myCollection.IndexExists(keyNames))
                {
                    myCollection.EnsureIndex(keyNames);
                }
            }
        }
        #endregion
        


        #region
        public void Mongo()
        {
            //建立连接
            var client = new MongoClient();
            //建立数据库
            var database = client.GetDatabase("TestDb");
            //建立collection
            var collection = database.GetCollection<BsonDocument>("foo");

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
            Console.WriteLine(count);

            //查询数据
            var document1 = collection.Find(document);
            Console.WriteLine(document1.ToString());

            //更新数据
            var filter = Builders<BsonDocument>.Filter.Eq("name", "MongoDB");
            var update = Builders<BsonDocument>.Update.Set("name", "Ghazi");

            collection.UpdateMany(filter, update);

            //删除数据
            var filter1 = Builders<BsonDocument>.Filter.Eq("count", 101);

            collection.DeleteMany(filter1);

            BsonDocument document2 = new BsonDocument();
            document2.Add("name", "MongoDB");
            document2.Add("type", "Database");
            document2.Add("count", "1");

            collection.InsertOne(document2);
        }

        #endregion
        */
    }
}