using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;

namespace Dal
{
    public class DBHelperOracle : IDBHelper
    {
        public OracleConnection  conn = null;
        private OracleCommand cmd = null;
        private OracleDataAdapter adapter = null;
        private OracleDataReader dbreader = null;
        private bool isTrans = false;
        private OracleTransaction trans = null;
        private string dbType = string.Empty;
        private string connStr = string.Empty;

        public DBHelperOracle() { }

        public DBHelperOracle(string dbtype)
        {
            this.dbType = dbtype;
        }

        public DBHelperOracle(string dbtype, string StrConn) :
            this()
        {
            connStr = StrConn;
        }
        /**
         * 获取数据库连接对象
         * */
        public void getConn()
        {
            this.conn = new OracleConnection(connStr);
        }

        /**
         * 打开数据库连接
         * */
        public void openConn()
        {
            this.getConn();
            if ((this.conn != null) && (this.conn.State == ConnectionState.Closed))
            {
                try
                {
                    this.conn.Open();
                }
                catch (Exception ex)
                {

                    throw;
                }
               
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
            this.cmd.CommandText = sql;
            object returnval = cmd.ExecuteScalar();
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
                catch (Exception ex)
                {
                    this.conn = new OracleConnection(connStr);
                    this.openConn();
                }
                this.cmd = conn.CreateCommand();
            }
            cmd.CommandText = sql;
            adapter = new OracleDataAdapter(cmd);

            adapter.Fill(dt);

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
                    this.conn = new OracleConnection(connStr);
                    this.openConn();
                }
                this.cmd = conn.CreateCommand();
            }
            cmd.CommandText = sql;
            adapter = new OracleDataAdapter(cmd);
            adapter.Fill(dt);
            //this.closeConn();
            return dt;
        }

        #endregion
    }
}
