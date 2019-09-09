using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using LitJson;

namespace com.jove.sqlite
{
    /**
     * 学习日志管理
     */
    public static class StudyLogManager
    {
        public static void SynClsAndChsLog(string uid,List<Dictionary<string, object>> cls
            ,List<Dictionary<string, object>> chs)
        {
            if (!string.IsNullOrEmpty(uid))
            {
                Task.Run(delegate
                {
                     if (null != cls && 0 < cls.Count)
                     {
                         using (var reader = SqliteOpenHelper.GetInstance().Query("update_class"
                             , new[] {"_id", "uid", "u_cls_id", "is_need_sync"}
                             , $"uid = '{uid}'"))
                         {
                             var idM = new Dictionary<string, int>();
                             var syncM = new Dictionary<string, int>();
                             if (null != reader)
                             {
                                 while (reader.Read())
                                 {
                                     var id = reader.GetInt32(0);
                                     var isNeedSync = reader.GetInt32(3);
                                     var key = $"{reader.GetString(1)}#{reader.GetString(2)}";
                                     idM[key] = id;
                                     syncM[key] = isNeedSync;
                                 }

                                 reader.Close();
                             }
                                
                             foreach (var item in cls)
                             {
                                 var columns = item.Keys.ToArray();
                                 var values = new object[item.Keys.Count];
                                 for (var i = 0; i < columns.Length; i++)
                                 {
                                     values[i] = item[columns[i]];
                                     if (null != values[i])
                                     {
                                         var data = values[i] as JsonData;
                                         if (null != data && data.IsString)
                                         {
                                             values[i] = data.ToString();
                                         }
                                     }
                                 }
                                 var key = $"{item["uid"]}#{item["u_cls_id"]}";
                                 if (syncM.ContainsKey(key))
                                 {
                                     if (1 != syncM[key])
                                     {
                                         SqliteOpenHelper.GetInstance().Update("update_class", columns, values
                                             , $"_id = {idM[key]}");
                                     }
                                 }
                                 else
                                 {
                                     SqliteOpenHelper.GetInstance()
                                         .Insert("update_class", columns, values);
                                 }
                             }
                         }
                     }

                     if (null != chs && 0 < chs.Count)
                     {
                         using (var reader = SqliteOpenHelper.GetInstance().Query("update_ch"
                             , new[] {"_id", "uid", "u_cls_id", "u_ch_id", "is_need_sync"}
                             , $"uid = '{uid}'"))
                         {
                             var idM = new Dictionary<string, int>();
                             var syncM = new Dictionary<string, int>();
                             if (null != reader)
                             {
                                 while (reader.Read())
                                 {
                                     var id = reader.GetInt32(0);
                                     var isNeedSync = reader.GetInt32(4);
                                     var key = $"{reader.GetString(1)}#{reader.GetString(2)}#{reader.GetString(3)}";
                                     idM[key] = id;
                                     syncM[key] = isNeedSync;
                                 }
                                 reader.Close();
                             }

                             foreach (var item in chs)
                             {
                                 var columns = item.Keys.ToArray();
                                 var values = new object[item.Keys.Count];
                                 for (var i = 0; i < columns.Length; i++)
                                 {
                                     values[i] = item[columns[i]];
                                     if (null != values[i])
                                     {
                                         var data = values[i] as JsonData;
                                         if (null != data && data.IsString)
                                         {
                                             values[i] = data.ToString();
                                         }
                                     }
                                 }
                                 var key = $"{item["uid"]}#{item["u_cls_id"]}#{item["u_ch_id"]}";
                                 if (syncM.ContainsKey(key))
                                 {
                                     if (1 != syncM[key])
                                     {
                                         SqliteOpenHelper.GetInstance().Update("update_ch", columns, values
                                             , $"_id = {idM[key]}");
                                     }
                                 }
                                 else
                                 {
                                     SqliteOpenHelper.GetInstance()
                                         .Insert("update_ch", columns, values);
                                 }
                             }
                         }
                     }
                });
            }
        }
    }
}