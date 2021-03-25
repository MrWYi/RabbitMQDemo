using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Dal
{
    public interface IDBHelper
    {
        void getConn();
        void openConn();
        void closeConn();
        void beginTrans();
        void commitTrans();
        void rollbackTrans();

        object execScalar(string sql);
        int execSql(string sql);
        int execSql(string sql, IDataParameter[] para);
        DataTable getDataTable(string sql);
        DataTable getDataTableNoAutoClose(string sql);
        IDataReader getDataReader(string sql);

      
    }

}
