using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel;
using System.Configuration;
namespace Kafka
{
    public partial class Consumer : INotifyPropertyChanged
    {
        Configuration conf_;
        public void InitConf()
        {
            conf_ = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            if (conf_.AppSettings.Settings["broker"] == null)
                conf_.AppSettings.Settings.Add("broker", "");
            broker_ = conf_.AppSettings.Settings["broker"].Value;
            if (conf_.AppSettings.Settings["topic"] == null)
                conf_.AppSettings.Settings.Add("topic", "");
            topic_ = conf_.AppSettings.Settings["topic"].Value;
            if (conf_.AppSettings.Settings["maxCnt"] == null)
                conf_.AppSettings.Settings.Add("maxCnt", "");
            cntMax_ = conf_.AppSettings.Settings["maxCnt"].Value;
            if (conf_.AppSettings.Settings["groupId"] == null)
                conf_.AppSettings.Settings.Add("groupId", "");
            groupId_ = conf_.AppSettings.Settings["groupId"].Value;
            if (conf_.AppSettings.Settings["logMaxLen"] == null)
                conf_.AppSettings.Settings.Add("logMaxLen", "1000000"); //1M
            logMaxLen_ = UInt64.Parse(conf_.AppSettings.Settings["logMaxLen"].Value);
            match_ = String.Empty;
            
        }
  
        public string GetConf(string key)
        {
            if (conf_ == null)
                InitConf();
            string ret=null;
            if(conf_.AppSettings.Settings[key] !=null)
             ret = conf_.AppSettings.Settings[key].Value;
            return ret;
        }
      
        public void SetConf(string key, string val)
        {
            if (conf_ == null)
                InitConf();
            conf_.AppSettings.Settings[key].Value = val;
        }
        public void SaveConf()
        {
            conf_.AppSettings.Settings["broker"].Value = broker_;
            conf_.AppSettings.Settings["topic"].Value = topic_;
            conf_.AppSettings.Settings["maxCnt"].Value = cntMax_;
             conf_.AppSettings.Settings["groupId"].Value = groupId_;
            conf_.Save();
        }
        private string _broker_;
        public string broker_
        {
            get { return _broker_; }
            set
            {
                _broker_ = value;
                OnPropertyChanged("broker_");
            }
        }
        private string _topic_;
        public string topic_
        {
            get { return _topic_; }
            set
            {
                _topic_ = value;
                OnPropertyChanged("topic_");
            }
        }

        private string _cntMax_;
        public string cntMax_
        {
            get { return _cntMax_; }
            set
            {
                _cntMax_ = value;
                OnPropertyChanged("cntMax_");
            }
        }
      
        private string _groupId_;
        public string groupId_
        {
            get { return _groupId_; }
            set
            {
                _groupId_ = value;
                OnPropertyChanged("groupId_");
            }
        }
        private UInt64 _matchCnt_;
        public UInt64 matchCnt_
        {
            get { return _matchCnt_; }
            set
            {
                _matchCnt_ = value;
                OnPropertyChanged("matchCnt_");
            }
        }
        private UInt64 _totalCnt_;
        public UInt64 totalCnt_
        {
            get { return _totalCnt_; }
            set
            {
                _totalCnt_ = value;
                OnPropertyChanged("totalCnt_");
            }
        }
        public List<string> topicLi_ { get; set; }
        public bool save2File_ { get; set; }
        public string match_ { get; set; }
        public bool regular_ { get; set; }
        public event PropertyChangedEventHandler PropertyChanged;
        public UInt64 logMaxLen_;
        protected virtual void OnPropertyChanged(string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
        
    }
    
}
