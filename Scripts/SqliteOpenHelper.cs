using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using Mono.Data.SqliteClient;
using UnityEngine;
using Task = System.Threading.Tasks.Task;

namespace com.jove.sqlite
{
    /**
     * 数据库打开 Helper
     */
    public class SqliteOpenHelper
    {
        private static readonly object SqliteLock = new object();
        private static SqliteOpenHelper _instance;
        private SqliteConnection _connection;
        private static readonly string DatabaseName = (Application.identifier??"sqlite")+".db";
        private const int DatabaseVersion = 1;
        private static readonly string DatabasePath = Application.persistentDataPath;
        
        private SqliteOpenHelper()
        {
            OpenInitDatabase();
        }
        
        private void OpenInitDatabase()
        {
           _connection = new SqliteConnection($"URI=file:{DatabasePath}/{DatabaseName}");
           _connection.Open();
           using (var command = _connection.CreateCommand())
           {
               command.CommandText = "PRAGMA COUNT_CHANGES = TRUE";
               command.ExecuteReader();
               using (var transaction = _connection.BeginTransaction(IsolationLevel.ReadCommitted))
               {
                   try
                   {
                       command.CommandText = "PRAGMA user_version;";
                       using (var reader = command.ExecuteReader())
                       {
                           if (reader.Read())
                           {
                               var version = reader.GetInt32(0);
                               Debug.Log($"db version {version}");
                               if (0 == version)
                               {
                                 
                                   OnCreate(_connection);
                                   command.CommandText = $"PRAGMA user_version = {DatabaseVersion}";
                                   command.ExecuteNonQuery();
                               }
                               else if (version < DatabaseVersion)
                               {
                                   OnUpgrade(_connection, version, DatabaseVersion);
                                   command.CommandText = $"PRAGMA user_version = {DatabaseVersion}";
                                   command.ExecuteNonQuery();
                               }
                           }
                       }

                       transaction.Commit();
                   }
                   catch (Exception e)
                   {
                       transaction.Rollback();
                       Debug.Log($"数据库打开异常：{e.Message}");
                       Debug.LogError(e);
                   }
               }
           }
        }

        
        private static void OnCreate(DbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = BuildSqlCreateTableUpdateClass();
                command.ExecuteNonQuery();
                command.CommandText = BuildSqlCreateTableUpdateCh();
                command.ExecuteNonQuery();
                command.CommandText = BuildSqlCreateTableRecCls();
                command.ExecuteNonQuery();
                command.CommandText = BuildSqlCreateTableRecChs();
                command.ExecuteNonQuery();
                command.CommandText = BuildSqlCreateTableRecWrong();
                command.ExecuteNonQuery();
                command.CommandText = BuildSqlCreateTableCoin();
                command.ExecuteNonQuery();
                command.CommandText = BuildSqlCreateTableBabyInfo();
                command.ExecuteNonQuery();
                command.CommandText = BuildSqlCreateTableLearnLog();
                command.ExecuteNonQuery();
                command.CommandText = BuildSqlCreateTableAnalyzeData();
                command.ExecuteNonQuery();
            }
        }
        
        private static void OnUpgrade(SqliteConnection connection
            , int oldVersion, int newVersion)
        {
            
        }

        public static IEnumerator<LinkedList<Dictionary<string,object>>> AsyncQuery(string tableName, string[] columns
            ,Action<LinkedList<Dictionary<string,object>>> callback
            , string where = "", string orderBy = "",int limit = -1)
        {
            return new AsyncQueryJob(tableName,columns,callback,where,orderBy,limit);
        }

        private class AsyncQueryJob:IEnumerator<LinkedList<Dictionary<string,object>>>
        {
            private readonly Action<LinkedList<Dictionary<string,object>>> _callback;
            private Task _task;
            
            public AsyncQueryJob(string tableName, string[] columns
                ,Action<LinkedList<Dictionary<string,object>>> callback
                , string where = "", string orderBy = "",int limit = -1)
            {
                _callback = callback;
               _task =  Task.Run(delegate
                {
                    using (var reader = GetInstance().Query(tableName, columns, where
                        , orderBy,limit))
                    {
                        Current = new LinkedList<Dictionary<string, object>>();
                        if (null != reader)
                        {
                            while (reader.Read())
                            {
                                var item = new Dictionary<string, object>();
                                for (var i = 0; i < reader.FieldCount; i++)
                                {
                                    item[reader.GetName(i)] = reader.GetValue(i);
                                }
                                Current.AddLast(item);
                            }
                        }   
                    }
                });
            }

            public bool MoveNext()
            {
                if (_task.IsCompleted)
                {
                    _callback?.Invoke(Current);
                }
                return !_task.IsCompleted;
            }

            public void Reset()
            {
                _task = null;
                Current = null;
            }

            public LinkedList<Dictionary<string, object>> Current
            {
                get; private set;
            }

            object IEnumerator.Current => Current;

            public void Dispose()
            {
                Reset();
            }
        }
        

        public int Delete(string tableName, string where = "")
        {
            lock (SqliteLock)
            {
                using (var transaction = _connection.BeginTransaction())
                {
                    try
                    {
                        using (var command = _connection.CreateCommand())
                        {
                            var sql = new StringBuilder();
                            sql.Append($"DELETE FROM {tableName} ");
                            if (!string.IsNullOrEmpty(where))
                            {
                                sql.Append($" WHERE {where}");
                            }
                            sql.Append(";");
                            command.CommandText = sql.ToString();
                            using (var reader = command.ExecuteReader())
                            {
                                transaction.Commit();
                                if (reader.Read())
                                {
                                    return reader.GetInt32(0);
                                }
                            }
                            
                        }
                    }
                    catch (Exception e)
                    {
                        Debug.LogError(e); 
                        transaction.Rollback();
                    }
                }
                return 0;
            }
        }

        public DbDataReader Query(string tableName,string[] columns
            ,string where = "",string orderBy = "",int limit = -1)
        {
            lock (SqliteLock)
            {
                try
                {
                    using (var command = _connection.CreateCommand())
                    {
                        var sql = new StringBuilder();
                        sql.Append("SELECT ");
                        if (null != columns)
                        {
                            for (var i = 0; i < columns.Length; i++)
                            {
                                sql.Append($"{columns[i]}");
                                if (i != columns.Length - 1)
                                {
                                    sql.Append(",");
                                }
                            }
                            sql.Append(" ");
                        }
                        else
                        {
                            sql.Append("* ");
                        }
                        sql.Append($"FROM {tableName}");
                        if (!string.IsNullOrEmpty(where))
                        {
                            sql.Append($" WHERE {where} ");
                        }
                        if (!string.IsNullOrEmpty(orderBy))
                        {
                            sql.Append($" ORDER BY {orderBy} ");
                        }
                        if (limit > 0)
                        {
                            sql.Append($" LIMIT {limit} ");
                        }
                        sql.Append(";");
                        command.CommandText = sql.ToString();
                        return command.ExecuteReader();
                    }
                }
                catch(Exception e)
                {
                    Debug.LogError(e);
                }
                return null;
            }
        }

        public DbDataReader Query(string tableName)
        {
            lock (SqliteLock)
            {
                try
                {
                    using (var command = _connection.CreateCommand())
                    {
                        command.CommandText = $"SELECT * FROM {tableName};";
                        return command.ExecuteReader();
                    }
                }
                catch(Exception e)
                {
                    Debug.LogError(e);
                }
                return null;
            }
        }


        public int Update(string tabName,string[] columns,object[] values,string where = "")
        {
            lock (SqliteLock)
            {
                if (null != columns && null != values && columns.Length == values.Length)
                {
                    using (var transaction = _connection
                        .BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        try
                        {
                            using (var command = _connection.CreateCommand())
                            {
                                var sql = new StringBuilder();
                                sql.Append($"UPDATE {tabName} ");
                                sql.Append(" SET ");
                           
                                for (var i = 0; i < columns.Length; i++)
                                {
                                    sql.Append(string.Format((null != values[i] && values[i] is string
                                            ?"{0} = '{1}'":"{0} = {1}"), columns[i], values[i]));
                                    if (i != columns.Length - 1)
                                    {
                                        sql.Append(",");
                                    }
                                }
                                if (!string.IsNullOrEmpty(where))
                                {
                                    sql.Append($" WHERE {where} ");
                                }
                                sql.Append(";");
                                command.CommandText = sql.ToString();
                                using (var reader = command.ExecuteReader())
                                {
                                    transaction.Commit();
                                    if (reader.Read())
                                    {
                                        return reader.GetInt32(0);
                                    }
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            transaction.Rollback();
                            Debug.LogError(e);
                        }
                    }
                }
            }
            return 0;
        }

        public int Insert(string tabName,string[] columns,object[] values)
        {
            lock (SqliteLock)
            {
                if (null != columns && null != values && values.Length == columns.Length)
                {
                    using ( var transaction = _connection
                        .BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        try
                        {
                            using (var command = _connection.CreateCommand())
                            {
                                var sql = new StringBuilder();
                                sql.Append($"INSERT INTO {tabName}");
                                sql.Append(" (");
                                for (var i = 0; i < columns.Length; i++)
                                {
                                    sql.Append($"{columns[i]}");
                                    if (i != columns.Length - 1)
                                    {
                                        sql.Append(",");
                                    }
                                }
                                sql.Append(") ");
                                sql.Append("VALUES");
                                sql.Append(" (");
                                for (var i = 0; i < values.Length; i++)
                                {
                                    sql.Append(string.Format((null != values[i] && values[i] is string)
                                        ?"'{0}'":"{0}", values[i]));
                                    if (i != values.Length - 1)
                                    {
                                        sql.Append(",");
                                    }
                                }
                                sql.Append(");");
                                command.CommandText = sql.ToString();
                                using (var reader = command.ExecuteReader())
                                {
                                    transaction.Commit();
                                    if (reader.Read())
                                    {
                                        return reader.GetInt32(0);
                                    }
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Debug.LogError(e);
                            transaction.Rollback();
                            return 0;
                        }
                    }
                }
                return 0;
            }
        }
      
        
        public static SqliteOpenHelper GetInstance()
        {
            if (null == _instance)
            {
                lock (SqliteLock)
                {
                    if (null == _instance)
                    {
                        _instance = new SqliteOpenHelper(); 
                    }
                }
            }
            return _instance;
        }
        
        
        
              
        private static readonly StringBuilder SqlCreateTableUpdateClass = new StringBuilder();

        private static string BuildSqlCreateTableUpdateClass()
        {
            SqlCreateTableUpdateClass.Remove(0, SqlCreateTableUpdateClass.Length);
            SqlCreateTableUpdateClass.Append("CREATE TABLE IF NOT EXISTS update_class (");
            SqlCreateTableUpdateClass.Append("_id INTEGER PRIMARY KEY,");
            SqlCreateTableUpdateClass.Append("uid TEXT NOT NULL,");
            SqlCreateTableUpdateClass.Append("u_cls_id TEXT NOT NULL,");
            SqlCreateTableUpdateClass.Append("u_cls_is_comp INTEGER DEFAULT 0,");
            SqlCreateTableUpdateClass.Append("u_cls_total_time LONG DEFAULT 0,");
            SqlCreateTableUpdateClass.Append("u_cls_total_times INTEGER DEFAULT 0,");
            SqlCreateTableUpdateClass.Append("u_cls_coin_can_get INTEGER DEFAULT 0,");
            SqlCreateTableUpdateClass.Append("u_cls_score_avg INTEGER DEFAULT 0,");
            SqlCreateTableUpdateClass.Append("is_need_sync INTEGER DEFAULT 0,");
            SqlCreateTableUpdateClass.Append("modification_time LONG DEFAULT 0,");
            SqlCreateTableUpdateClass.Append("u_cls_unlock_exp INTEGER DEFAULT 0,");
            SqlCreateTableUpdateClass.Append("u_cls_max_exp INTEGER DEFAULT 0,");
            SqlCreateTableUpdateClass.Append("u_cls_current_total_exp INTEGER DEFAULT 0,");
            SqlCreateTableUpdateClass.Append("u_cls_medal_exp INTEGER DEFAULT 0");
            SqlCreateTableUpdateClass.Append(");");
            return SqlCreateTableUpdateClass.ToString();
        }

        private static readonly StringBuilder SqlCreateTableUpdateCh = new StringBuilder();
        
        private static string BuildSqlCreateTableUpdateCh()
        {
            SqlCreateTableUpdateCh.Remove(0, SqlCreateTableUpdateCh.Length);
            SqlCreateTableUpdateCh.Append("CREATE TABLE IF NOT EXISTS update_ch (");
            SqlCreateTableUpdateCh.Append("_id INTEGER PRIMARY KEY,");
            SqlCreateTableUpdateCh.Append("uid TEXT NOT NULL,");
            SqlCreateTableUpdateCh.Append("u_cls_id TEXT NOT NULL,");
            SqlCreateTableUpdateCh.Append("u_ch_id TEXT NOT NULL,");
            SqlCreateTableUpdateCh.Append("u_ch_is_comp INTEGER DEFAULT 0,");
            SqlCreateTableUpdateCh.Append("u_ch_total_time LONG DEFAULT 0,");
            SqlCreateTableUpdateCh.Append("u_ch_total_times INTEGER DEFAULT 0,");
            SqlCreateTableUpdateCh.Append("modification_time LONG DEFAULT 0,");
            SqlCreateTableUpdateCh.Append("u_ch_coin_can_get INTEGER DEFAULT 0,");
            SqlCreateTableUpdateCh.Append("u_ch_score_highest INTEGER DEFAULT 0,");
            SqlCreateTableUpdateCh.Append("is_need_sync INTEGER DEFAULT 0,");
            SqlCreateTableUpdateCh.Append("u_ch_total_times_last INTEGER DEFAULT 0");
            SqlCreateTableUpdateCh.Append(");");
            return SqlCreateTableUpdateCh.ToString();
        }
        
        private static readonly StringBuilder SqlCreateTableRecCls = new StringBuilder();
        
        private static string BuildSqlCreateTableRecCls()
        {
            SqlCreateTableRecCls.Remove(0, SqlCreateTableRecCls.Length);
            SqlCreateTableRecCls.Append("CREATE TABLE IF NOT EXISTS rec_class (");
            SqlCreateTableRecCls.Append("_id INTEGER PRIMARY KEY,");
            SqlCreateTableRecCls.Append("uid TEXT NOT NULL,");
            SqlCreateTableRecCls.Append("play_id TEXT NOT NULL,");
            SqlCreateTableRecCls.Append("u_cls_id TEXT NOT NULL,");
            SqlCreateTableRecCls.Append("u_be_start_time LONG DEFAULT 0,");
            SqlCreateTableRecCls.Append("u_be_end_time LONG DEFAULT 0,");
            SqlCreateTableRecCls.Append("u_cls_study_time LONG DEFAULT 0,");
            SqlCreateTableRecCls.Append("time LONG DEFAULT 0");
            SqlCreateTableRecCls.Append(");");
            return SqlCreateTableRecCls.ToString();
        }

        private static readonly StringBuilder SqlCreateTableRecChs = new StringBuilder();
        
        private static string BuildSqlCreateTableRecChs()
        {
            SqlCreateTableRecChs.Remove(0, SqlCreateTableRecChs.Length);
            SqlCreateTableRecChs.Append("CREATE TABLE IF NOT EXISTS rec_ch (");
            SqlCreateTableRecChs.Append("_id INTEGER PRIMARY KEY,");
            SqlCreateTableRecChs.Append("uid TEXT NOT NULL,");
            SqlCreateTableRecChs.Append("play_id TEXT NOT NULL,");
            SqlCreateTableRecChs.Append("u_cls_id TEXT NOT NULL,");
            SqlCreateTableRecChs.Append("u_ch_id TEXT NOT NULL,");
            SqlCreateTableRecChs.Append("u_be_start_time LONG DEFAULT 0,");
            SqlCreateTableRecChs.Append("u_be_end_time LONG DEFAULT 0,");
            SqlCreateTableRecChs.Append("u_be_ch_is_exist INTEGER DEFAULT 0,");
            SqlCreateTableRecChs.Append("u_ch_wrong_time LONG DEFAULT 0,");
            SqlCreateTableRecChs.Append("u_ch_wrong_times INTEGER DEFAULT 0,");
            SqlCreateTableRecChs.Append("u_ch_score INTEGER DEFAULT 0,");
            SqlCreateTableRecChs.Append("u_ch_coin INTEGER DEFAULT 0,");
            SqlCreateTableRecChs.Append("u_cls_study_time LONG DEFAULT 0,");
            SqlCreateTableRecChs.Append("time LONG DEFAULT 0");
            SqlCreateTableRecChs.Append(");");
            return SqlCreateTableRecChs.ToString();
        }


        private static readonly StringBuilder SqlCreateTableRecWrong = new StringBuilder();
        
        private static string BuildSqlCreateTableRecWrong()
        {
            SqlCreateTableRecWrong.Remove(0, SqlCreateTableRecWrong.Length);
            SqlCreateTableRecWrong.Append("CREATE TABLE IF NOT EXISTS rec_wrong (");
            SqlCreateTableRecWrong.Append("_id INTEGER PRIMARY KEY,");
            SqlCreateTableRecWrong.Append("uid TEXT NOT NULL,");
            SqlCreateTableRecWrong.Append("play_id TEXT NOT NULL,");
            SqlCreateTableRecWrong.Append("u_cls_id TEXT NOT NULL,");
            SqlCreateTableRecWrong.Append("u_ch_id TEXT NOT NULL,");
            SqlCreateTableRecWrong.Append("study_id TEXT NOT NULL,");
            SqlCreateTableRecWrong.Append("u_study_wrong_times INTEGER DEFAULT 0,");
            SqlCreateTableRecWrong.Append("time LONG DEFAULT 0");
            SqlCreateTableRecWrong.Append(");");
            return SqlCreateTableRecWrong.ToString();
        }
        

        private static readonly StringBuilder SqlCreateTableCoin = new StringBuilder();
        
        private static string BuildSqlCreateTableCoin()
        {
            SqlCreateTableCoin.Remove(0, SqlCreateTableCoin.Length);
            SqlCreateTableCoin.Append("CREATE TABLE IF NOT EXISTS ch_coin (");
            SqlCreateTableCoin.Append("_id INTEGER PRIMARY KEY,");
            SqlCreateTableCoin.Append("uid TEXT NOT NULL,");
            SqlCreateTableCoin.Append("u_cls_id TEXT NOT NULL,");
            SqlCreateTableCoin.Append("u_ch_id TEXT NOT NULL,");
            SqlCreateTableCoin.Append("u_ch_coin INTEGER DEFAULT 0,");
            SqlCreateTableCoin.Append("u_time LONG DEFAULT 0");
            SqlCreateTableCoin.Append(");");
            return SqlCreateTableCoin.ToString();
        }
        
        private static readonly StringBuilder SqlCreateTableBabyInfo = new StringBuilder();

        private static string BuildSqlCreateTableBabyInfo()
        {
            SqlCreateTableBabyInfo.Remove(0, SqlCreateTableBabyInfo.Length);
            SqlCreateTableBabyInfo.Append("CREATE TABLE IF NOT EXISTS baby_info (");
            SqlCreateTableBabyInfo.Append("_id INTEGER PRIMARY KEY,");
            SqlCreateTableBabyInfo.Append("uid TEXT NOT NULL,");
            SqlCreateTableBabyInfo.Append("name TEXT,");
            SqlCreateTableBabyInfo.Append("nickname TEXT,");
            SqlCreateTableBabyInfo.Append("sex INTEGER DEFAULT 0,");
            SqlCreateTableBabyInfo.Append("portrait TEXT,");
            SqlCreateTableBabyInfo.Append("createTime LONG,");
            SqlCreateTableBabyInfo.Append("birthday LONG,");
            SqlCreateTableBabyInfo.Append("is_need_sync DEFAULT 1,");
            SqlCreateTableBabyInfo.Append("coin_total INTEGER DEFAULT 0,");
            SqlCreateTableBabyInfo.Append("last_signin_time INTEGER DEFAULT 0");
            SqlCreateTableBabyInfo.Append(");");
            return SqlCreateTableBabyInfo.ToString();
        }

        
        private static readonly StringBuilder SqlCreateTableLearnLog = new StringBuilder();

        private static string BuildSqlCreateTableLearnLog()
        {
            SqlCreateTableLearnLog.Remove(0, SqlCreateTableLearnLog.Length);
            SqlCreateTableLearnLog.Append("CREATE TABLE IF NOT EXISTS learn_log (");
            SqlCreateTableLearnLog.Append("_id INTEGER PRIMARY KEY,");
            SqlCreateTableLearnLog.Append("uid TEXT NOT NULL,");
            SqlCreateTableLearnLog.Append("id TEXT NOT NULL,");
            SqlCreateTableLearnLog.Append("status INTEGER DEFAULT 0,");
            SqlCreateTableLearnLog.Append("event_id TEXT UNIQUE NOT NULL,");
            SqlCreateTableLearnLog.Append("description TEXT,");
            SqlCreateTableLearnLog.Append("event_time TEXT DEFAULT (strftime('%s',datetime('now','localtime'))),");
            SqlCreateTableLearnLog.Append("modified TEXT DEFAULT (strftime('%s',datetime('now','localtime'))),");
            SqlCreateTableLearnLog.Append("event TEXT,");
            SqlCreateTableLearnLog.Append("score TEXT");
            SqlCreateTableLearnLog.Append(");");
            return SqlCreateTableLearnLog.ToString();
        }
        
        
        
        private static readonly StringBuilder SqlCreateTableAnalyzeData = new StringBuilder();

        private static string BuildSqlCreateTableAnalyzeData()
        {
            SqlCreateTableAnalyzeData.Remove(0, SqlCreateTableAnalyzeData.Length);
            SqlCreateTableAnalyzeData.Append("CREATE TABLE IF NOT EXISTS analyze_data (");
            SqlCreateTableAnalyzeData.Append("_id INTEGER PRIMARY KEY,");
            SqlCreateTableAnalyzeData.Append("uid TEXT NOT NULL,");
            SqlCreateTableAnalyzeData.Append("u_be_click_id TEXT UNIQUE NOT NULL,");
            SqlCreateTableAnalyzeData.Append("u_be_click_times INTEGER DEFAULT 0,");
            SqlCreateTableAnalyzeData.Append("u_be_click_start_time LONG DEFAULT 0,");
            SqlCreateTableAnalyzeData.Append("u_be_click_end_time LONG DEFAULT 0,");
            SqlCreateTableAnalyzeData.Append("is_need_sync INTEGER DEFAULT 0");
            SqlCreateTableAnalyzeData.Append(");");
            return SqlCreateTableAnalyzeData.ToString();
        }
    }
}
