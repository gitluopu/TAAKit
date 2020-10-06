using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel;

namespace Kafka
{
    public partial class Producer: INotifyPropertyChanged
    {
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

        public string msg_ { get; set; }
        public string cnt_ { get; set; }
       
        public string interval_ { get; set; }
        
        public event EventHandler<string> OnProduceCompleted;
        public event PropertyChangedEventHandler PropertyChanged;

        protected virtual void OnPropertyChanged(string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}
