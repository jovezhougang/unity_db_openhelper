using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using LitJson;
using UnityEngine;

namespace com.jove.sqlite
{
    public class SyncManager : MonoBehaviour
    {
        public static SyncManager Instance { get; private set;}

        private void Awake()
        {
            if (null == Instance)
            {
                DestroyImmediate(gameObject);
                return;
            }
            Instance = this;
            SqliteOpenHelper.GetInstance();
        }

        /**
         * 同步学习记录信息
         */
        public void SyncLearnLog(string uid)
        {
            if (!string.IsNullOrEmpty(uid))
            {
                StartCoroutine(SqliteOpenHelper.AsyncQuery("rec_class", null
                    , delegate(LinkedList<Dictionary<string, object>> recClassData)
                    {
                        StartCoroutine(SqliteOpenHelper.AsyncQuery("rec_ch", null
                            , delegate(LinkedList<Dictionary<string, object>> recChData)
                            {
                                StartCoroutine(SqliteOpenHelper.AsyncQuery("rec_wrong", null
                                    , delegate(LinkedList<Dictionary<string, object>> recWrongData)
                                    {
                                        var data = new Dictionary<string,object>(3);
                                        data["cls"] = recClassData;
                                        data["ch"] = recChData;
                                        data["errs"] = recWrongData;
                                        var dataJson = JsonMapper.ToJson(data);
                                        Debug.Log(dataJson);
                                        Task.Run(delegate
                                        {
                                            if (null != recClassData)
                                            {
                                                foreach (var item in recClassData)
                                                {
                                                    if (item.ContainsKey("_id"))
                                                    {
                                                        SqliteOpenHelper.GetInstance()
                                                            .Delete("rec_class"
                                                                , $"_id = {item["_id"]}");
                                                    }
                                                }
                                            }
                                            if (null != recChData)
                                            {
                                                foreach (var item in recChData)
                                                {
                                                    if (item.ContainsKey("_id"))
                                                    {
                                                        SqliteOpenHelper.GetInstance()
                                                            .Delete("rec_ch"
                                                                , $"_id = {item["_id"]}");
                                                    }
                                                }
                                            }
                                            if (null != recWrongData)
                                            {
                                                foreach (var item in recWrongData)
                                                {
                                                    if (item.ContainsKey("_id"))
                                                    {
                                                        SqliteOpenHelper.GetInstance()
                                                            .Delete("rec_wrong"
                                                                , $"_id = {item["_id"]}");
                                                    }
                                                }
                                            }
                                        });
                                    }
                                    , $"uid = {uid}",limit:20));
                            }
                            , $"uid = {uid}",limit:20));
                    }
                    , $"uid = {uid}",limit:20));
            }
        }

        public void SyncClsAndChs(string uid)
        {
            if (!string.IsNullOrEmpty(uid))
            {
                StartCoroutine(SqliteOpenHelper
                    .AsyncQuery("update_class", null
                        , delegate(LinkedList<Dictionary<string, object>> cls)
                        {
                            StartCoroutine(SqliteOpenHelper
                                .AsyncQuery("update_ch", null
                                    , delegate(LinkedList<Dictionary<string, object>> chs)
                                    {
                                        JsonMapper.ToJson(cls);
                                        JsonMapper.ToJson(chs);
                                        Task.Run(delegate
                                        {
                                            foreach (var item in cls)
                                            {
                                                SqliteOpenHelper.GetInstance()
                                                    .Update("update_class", new string[] {"is_need_sync"}
                                                        , new object[] {0},
                                                        $"_id = {item["_id"]} AND is_need_sync = {0}");
                                            }
                                            foreach (var item in chs)
                                            {
                                                SqliteOpenHelper.GetInstance()
                                                    .Update("update_ch", new string[] {"is_need_sync"}
                                                        , new object[] {0},
                                                        $"_id = {item["_id"]} AND is_need_sync = {0}");
                                            }
                                        });
                                    }
                                    , $"is_need_sync = {1} AND uid = {uid}"));
                        }
                        , $"is_need_sync = {1} AND uid = {uid}",limit:20)); 
            }
        }

        /**
         * 同步金币获取记录信息
         */
        public void SyncCoinInfo(string uid)
        {
            if (!string.IsNullOrEmpty(uid))
            {
             StartCoroutine(SqliteOpenHelper.AsyncQuery("ch_coin", null
                    , delegate(LinkedList<Dictionary<string, object>> coinData)
                    {
                        //TODO 上传金币记录到服务器
                        Task.Run(delegate
                        {
                            var sb = new StringBuilder();
                            foreach (var item in coinData)
                            {
                                if (item.ContainsKey("_id"))
                                {
                                    sb.Append(item["_id"]);
                                    sb.Append(",");
                                }
                            }
                            if (sb.Length > 0)
                            {
                                SqliteOpenHelper.GetInstance().Delete("ch_coin"
                                        , $"_id in ({sb.Remove(sb.Length - 1, 1)})");
                            }
                        });
                    }, $"uid = {uid}",limit:20));
            }
        }
    }
}