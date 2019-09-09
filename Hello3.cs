using System.Collections;
using System.Collections.Generic;
using System.IO;
using com.jove.sqlite;
using UnityEngine;
using LitJson;
using Object = System.Object;

public class Hello3 : MonoBehaviour
{
	private RectTransform _transform;
	void Start ()
	{
		_transform = transform as RectTransform;
		
		if (_transform != null)
		{Debug.Log(_transform.rect.width);
			_transform.anchorMin = new Vector2(1,1);
			Debug.Log(_transform.rect.width);
			_transform.anchorMax = new Vector2(1,1);
			_transform.pivot = Vector2.one;
			_transform.anchoredPosition = new Vector2(2000,100);

			Debug.Log(transform.TransformPoint(new Vector3(0, 0, 0)));
		}

		
		SqliteOpenHelper.GetInstance();
		StartCoroutine(Init());
		
		Debug.Log(string.Format("IS IPhonePlayer ==>{0}"
			,Application.platform == RuntimePlatform.IPhonePlayer));
		Debug.Log(string.Format("File ===>{0}"
			,Directory.Exists(Application.streamingAssetsPath +"/lessons")));

		foreach (var file in Directory.GetFiles(string.Format("{0}/{1}"
			, Application.streamingAssetsPath, "lessons")))
		{
			Debug.Log(string.Format("File path==>{0}",file));
		}
	}

	private IEnumerator Init()
	{
		var wwwForm = new WWWForm();
		wwwForm.AddField("user.phone","18307207411");
		wwwForm.AddField("user.password","123456");
		wwwForm.AddField("deviceType","android");
		wwwForm.AddField("app_pkg_name","com.mytian.appstore.rz");
		wwwForm.AddField("app_channel", "mytian");
		wwwForm.AddField("client_version", "82");
		var www = new WWW("http://www.mytian.com.cn/myt_market/userAction_login.do"
			,wwwForm);
		yield return www;
		var jsonData = JsonMapper.ToObject(www.text);

		
		var cls = new List<Dictionary<string, object>>();
		for (var i = 0; i < jsonData["cls"].Count; i++)
		{
			var data = jsonData["cls"][i];
			var item = new Dictionary<string,object>(data.Keys.Count);
			cls.Add(item);
			foreach (var dataKey in data.Keys)
			{
				item[dataKey] = data[dataKey];
			}
		}
		
		var chs = new List<Dictionary<string, object>>();
		for (var i = 0; i < jsonData["ch"].Count; i++)
		{
			var data = jsonData["ch"][i];
			var item = new Dictionary<string,object>(data.Keys.Count);
			chs.Add(item);
			foreach (var dataKey in data.Keys)
			{
				item[dataKey] = data[dataKey];
			}
		}
		StudyLogManager.SynClsAndChsLog(cls[0]["uid"]+"",cls,chs);

		StartCoroutine(SqliteOpenHelper.AsyncQuery("update_class", null, AsyncQueryCallback));
	}


	private void AsyncQueryCallback(LinkedList<Dictionary<string,object>> reader)
	{
		Debug.Log($"AsyncQueryCallback {reader}");
		if (null != reader)
		{
			Debug.Log(reader.Count);
		}
	}
}
