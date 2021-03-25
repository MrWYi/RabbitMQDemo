using MySql.Data.MySqlClient;
using System;
using System.Data;

namespace Dal
{
    public class DBHelperMySQL : IDBHelper
    {
        public MySqlConnection conn = null;
        private MySqlCommand cmd = null;
        private MySqlDataAdapter adapter = null;
        private MySqlDataReader dbreader = null;
        private bool isTrans = false;
        private MySqlTransaction trans = null;
        private string dbType = string.Empty;
        private string connStr = string.Empty;

        public DBHelperMySQL() { }

        public DBHelperMySQL(string dbtype)
        {
            this.dbType = dbtype;
        }

        public DBHelperMySQL(string dbtype, string StrConn) :
            this()
        {
            connStr = StrConn;
        }
        /**
         * 获取数据库连接对象
         * */
        public void getConn()
        {
            this.conn = new MySqlConnection(connStr);
        }

        /**
         * 打开数据库连接
         * */
        public void openConn()
        {
            this.getConn();
            if ((this.conn != null) && (this.conn.State == ConnectionState.Closed))
            {
                this.conn.Open();
            }
        }

        /**
         *  关闭数据库连接
         * */
        public void closeConn()
        {
            if ((this.conn != null) && (this.conn.State != ConnectionState.Closed) && !this.isTrans)
            {
                this.conn.Close();
                this.conn.Dispose();
            }
        }

        /**
         * 开始事务
         * */
        public void beginTrans()
        {
            this.openConn();
            this.isTrans = true;
            this.trans = this.conn.BeginTransaction();
        }

        /**
         * 提交事务
         * */
        public void commitTrans()
        {
            this.trans.Commit();
            this.isTrans = false;
            this.closeConn();
        }

        /**
         * 回滚事务
         * */
        public void rollbackTrans()
        {
            this.trans.Rollback();
            this.isTrans = false;
            this.closeConn();
        }

        /**
         * 执行DML语句
         * */
        public int execSql(string sql)
        {
            if (this.isTrans)
            {
                this.cmd = conn.CreateCommand();
                this.cmd.Transaction = this.trans;
            }
            else
            {
                this.openConn();
                this.cmd = conn.CreateCommand();
            }

            this.cmd.CommandTimeout = 240;
            this.cmd.CommandText = sql;
            int result = this.cmd.ExecuteNonQuery();
            this.closeConn();
            return result;
        }

        /**
         * 执行DML语句（可带参数）
         * */
        public int execSql(string sql, IDataParameter[] para)
        {
            if (this.isTrans)
            {
                this.cmd = conn.CreateCommand();
                this.cmd.Transaction = this.trans;
            }
            else
            {
                this.openConn();
                this.cmd = conn.CreateCommand();
            }

            cmd.CommandTimeout = 240;
            this.cmd.CommandText = sql;
            // this.cmd.CommandType = CommandType.StoredProcedure;
            for (int i = 0; i < para.Length; i++)
            {
                this.cmd.Parameters.Add(para[i]);
            }
            int result = this.cmd.ExecuteNonQuery();
            this.closeConn();
            return result;
        }

        /**
         * 执行DML语句，并返回单独一列的值
         * */
        public object execScalar(string sql)
        {
            if (this.isTrans)
            {
                this.cmd = conn.CreateCommand();
                this.cmd.Transaction = this.trans;
            }
            else
            {
                this.openConn();
            }
            this.cmd = conn.CreateCommand();
            cmd.CommandTimeout = 240;
            this.cmd.CommandText = sql;
            object returnval = null;
            try
            {
                 returnval = cmd.ExecuteScalar();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message + ": " + sql);
               
            }
           
            this.closeConn();
            return returnval;
        }


        /**
         * 通过SQL语句返回结果集
         * */
        public DataTable getDataTable(string sql)
        {
            DataTable dt = new DataTable();
            if (this.isTrans)
            {
                this.cmd = conn.CreateCommand();
                this.cmd.Transaction = this.trans;
            }
            else
            {
                try
                {
                    this.openConn();
                }
                catch (Exception)
                {
                    this.conn = new MySqlConnection(connStr);
                    this.openConn();
                }
                this.cmd = conn.CreateCommand();
                
            }
            adapter = new MySqlDataAdapter();
            try
            {
                cmd.CommandTimeout = 99999;
                cmd.CommandText = sql;
                adapter.SelectCommand = cmd;
                adapter.Fill(dt);
                adapter.Dispose();
            }
            catch (MySql.Data.MySqlClient.MySqlException e)
            {
                Console.WriteLine(e.Message+":"+sql);
            }
            this.closeConn();
            return dt;
        }



        public IDataReader getDataReader(string sql)
        {
            this.openConn();
            this.cmd = conn.CreateCommand();

            cmd.CommandText = sql;
            this.dbreader = cmd.ExecuteReader();
            return dbreader;
        }

        #region IDBHelper 成员


        public DataTable getDataTableNoAutoClose(string sql)
        {
            DataTable dt = new DataTable();
            if (this.isTrans)
            {
                this.cmd = conn.CreateCommand();
                this.cmd.Transaction = this.trans;
            }
            else
            {
                try
                {
                    this.openConn();
                }
                catch (Exception)
                {
                    this.conn = new MySqlConnection(connStr);
                    this.openConn();
                }
                this.cmd = conn.CreateCommand();
            }
            cmd.CommandText = sql;
            adapter = new MySqlDataAdapter(cmd);
            adapter.Fill(dt);
            //this.closeConn();
            return dt;
        }

        #region IDisposable Support
        private bool disposedValue = false; // 要检测冗余调用

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: 释放托管状态(托管对象)。
                }

                // TODO: 释放未托管的资源(未托管的对象)并在以下内容中替代终结器。
                // TODO: 将大型字段设置为 null。

                disposedValue = true;
            }
        }

        // TODO: 仅当以上 Dispose(bool disposing) 拥有用于释放未托管资源的代码时才替代终结器。
        // ~DBHelperMySQL() {
        //   // 请勿更改此代码。将清理代码放入以上 Dispose(bool disposing) 中。
        //   Dispose(false);
        // }

        // 添加此代码以正确实现可处置模式。
        public void Dispose()
        {
            // 请勿更改此代码。将清理代码放入以上 Dispose(bool disposing) 中。
            Dispose(true);
            // TODO: 如果在以上内容中替代了终结器，则取消注释以下行。
            // GC.SuppressFinalize(this);
        }

        public IDbConnection GetConnection()
        {
            return this.conn;
        }
        #endregion

        #endregion
    }
}
