using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using Confluent.Kafka;
using System.Net.Sockets;
using System.IO;
using System.Threading;
using System.Text.RegularExpressions;

namespace Kafka
{
    /// <summary>
    /// Interaction logic for UserControl1.xaml
    /// </summary>
    /// 
    public class BrokerAddr
    {
        public BrokerAddr(string _broker)
        {
            if (_broker.Contains(":"))
            {
                string[] strAry = _broker.Split(':');
                ip_ = strAry[0];
                port_ = strAry[1];
                broker_ = _broker;
            }
            else
            {
                ip_ = _broker;
                port_ = 9092.ToString();
                broker_ = ip_ + ":" + port_;
            }
        }
        
        public string broker_;
        public string ip_;
        public string port_;
    }
    public class Utils
    {
         public static void TcpPortTest(string ip, string _port, int ms)
        {
            int port = int.Parse(_port);
            using (var tcpClient = new TcpClient())
            {
                try
                {
                    if (!tcpClient.ConnectAsync(ip, port).Wait(ms))
                    {
                        throw new Exception("connect to " + ip + ":" + port + " timeout within 3000ms");
                    }
                }
                catch (Exception ex)
                {
                    string msg = ex.Message;
                    if (ex.InnerException != null)
                    {
                        msg += ex.InnerException.Message;
                    }
                    throw new Exception(msg);
                }
            }
        }
    }
   
    public partial class Consumer : UserControl
    {
        public Consumer()
        {
            InitializeComponent();
            SetLoggerBox(txtLog_);
            DataContext = this;
            InitConf();
            OnConsumeMsg += Write2Log;
            OnConsumeMsg += Write2File;

            topicLi_ = new List<string>();
            topicLi_.Add("audit");
            topicLi_.Add("alert");
            topicLi_.Add("traffic");
            topicLi_.Add("event");
            topicLi_.Add("PushResponse");

            ChangeStatusOnStop();
        }
         ~Consumer()
        {
            SaveConf();
        }
        private void Write2Log(object sender, string e)
        {
            txtLog_.Dispatcher.Invoke(() => {
                UInt64 len = (UInt64)(txtLog_.Text.Length + e.Length);
                if (len <= logMaxLen_)
                    txtLog_.AppendText(e + System.Environment.NewLine);
            });
        }
        //it's called in consume thread
        private void Write2File(object sender, string e)
        {
            if (!save2File_)
                return;
            if (writer_ == null)
            {
                BrokerAddr addr = new BrokerAddr(broker_);
                if(!Directory.Exists(cacheDir_))
                  Directory.CreateDirectory(cacheDir_);
                writer_ = new StreamWriter(cacheDir_ + addr.ip_ + "-" + topic_ + DateTime.Now.ToString("yyyy-MM-dd-hh-mm-ss") + ".log");
            }
            writer_.Write(e + System.Environment.NewLine);
        }
        public void SetLoggerBox(TextBox tb)
        {
            tb.TextWrapping = TextWrapping.Wrap;
            tb.AutoWordSelection = true;
            tb.IsReadOnly = true;
            tb.VerticalScrollBarVisibility = ScrollBarVisibility.Auto;
        }
        
        private bool Match(string msg)
        {
            bool match = false;
            if (match_.Length == 0)
                return true;
            if(regular_)
            {
                Regex rgx = new Regex(match_);
                if(rgx.IsMatch(msg))
                    match = true;
            }else
            {
                if(msg.Contains(match_))
                    match = true;
            }
            return match;
        }
        private void OnConsumeClick(object sender, RoutedEventArgs arg)
        {
            ChangeStatusOnConsume();
            BrokerAddr addr = new BrokerAddr(broker_);
            try
            {
                Utils.TcpPortTest(addr.ip_, addr.port_,3000);
            }
            catch (Exception ex)
            {
                btnConsume_.IsEnabled = true;
                btnStop_.IsEnabled = false;
                MessageBox.Show(ex.Message);
                ChangeStatusOnStop();
                return;
            }
            Task.Run(() =>
            {
                cts_ = new CancellationTokenSource();
                matchCnt_ = 0;
                totalCnt_ = 0;
                string groupId = null;
                if (groupId_ == "")
                    groupId = DateTime.Now.ToString();
                else
                    groupId = groupId_;
                var conf = new ConsumerConfig
                {
                    GroupId = groupId,
                    //AutoOffsetReset = AutoOffsetReset.Latest,
                    BootstrapServers = addr.broker_,
                    //SocketTimeoutMs = 1000,
                    //SessionTimeoutMs = 1000,
                    //MetadataRequestTimeoutMs=1000,
                    ReconnectBackoffMs = 1000,
                    ReconnectBackoffMaxMs = 3000,


                    // Note: The AutoOffsetReset property determines the start offset in the event
                    // there are not yet any committed offsets for the consumer group for the
                    // topic/partitions of interest. By default, offsets are committed
                    // automatically, so in this example, consumption will only start from the
                    // earliest message in the topic 'my-topic' the first time you run the program.
                    AutoOffsetReset = AutoOffsetReset.Latest
                };
                using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
                {
                    c.Subscribe(topic_);
                    try
                    {
                        while (true)
                        {
                            try
                            {
                                var cr = c.Consume(cts_.Token);
                                string msg = cr.Message.Value;
                                if(Match(msg))
                                {
                                    MatchInc();
                                    HandleMsg(msg);
                                }
                                if(CntIncThenContinue()==false)
                                {
                                    c.Close();
                                    break;
                                }
                               
                                Thread.Sleep(10);
                                //Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                            }
                            catch (ConsumeException ex)
                            {
                                MessageBox.Show(ex.Message);
                                //Log.LogErr(e.Error.Reason);
                                //Console.WriteLine($"Error occured: {e.Error.Reason}");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Ensure the consumer leaves the group cleanly and final offsets are committed.
                        c.Close();
                        //return;
                    }
                }
                //it is called in Write2File
                if (writer_ != null)
                { 
                    writer_.Close();
                    writer_ = null;
                }
                ChangeStatusOnStop();
            });
        }
        private void MatchInc()
        {
            matchCnt_++;
        }
        private bool CntIncThenContinue()
        {
            bool going = true;
            UInt64 cntMax = UInt64.Parse(cntMax_);
            totalCnt_++;
            if (cntMax > 0)
                if (totalCnt_ > cntMax)
                    going = false;
            return going;
        }
       
        private void HandleMsg(string msg)
        {
            OnConsumeMsg?.Invoke(this,msg);
        }


        private void OnStopClick(object sender, RoutedEventArgs arg)
        {
            cts_.Cancel();
            btnConsume_.IsEnabled = true;
            btnStop_.IsEnabled = false;
        }
        private void OnProducerClick(object sender, RoutedEventArgs arg)
        {
            Producer p = new Producer(this);
            p.Show();
        }
        private void OnClearClick(object sender, RoutedEventArgs arg)
        {
            txtLog_.Dispatcher.Invoke(() =>
            {
                txtLog_.Clear();
            });
        }
        private void ChangeStatus(bool b)
        {
            btnStop_.IsEnabled = !b;
            btnConsume_.IsEnabled = b;
            txtBroker_.IsEnabled = b;
            txtTopic_.IsEnabled = b;
            txtMaxCnt_.IsEnabled = b;
            txtMatch_.IsEnabled = b;
            chkRegular_.IsEnabled = b;
            chkSave2File_.IsEnabled = b;
            //txtLog_.IsEnabled = b;
            
        }

        private void ChangeStatusOnConsume()
        {
            btnConsume_.Dispatcher.Invoke(() => { ChangeStatus(false); });
        }
        private void ChangeStatusOnStop()
        {
            btnConsume_.Dispatcher.Invoke(() => { ChangeStatus(true); });
        }
        private CancellationTokenSource cts_;
        private StreamWriter writer_;
        public event EventHandler<string> OnConsumeMsg;

        const string cacheDir_ = "run/";
    }
}
