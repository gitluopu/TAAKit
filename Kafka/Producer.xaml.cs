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
using System.Windows.Shapes;
using Confluent.Kafka;
using System.Threading;
namespace Kafka
{
    /// <summary>
    /// Interaction logic for Producer.xaml
    /// </summary>
    public partial class Producer : Window
    {
        public Producer(Consumer c)
        {
            InitializeComponent();
            DataContext = this;
            OnProduceCompleted += ShowSendMsgBox;
            broker_ = c.broker_;
            topic_ = c.topic_;
            msg_ = "hello,kafka";
            cnt_ = 3.ToString();
            interval_ = 1000.ToString();
            ChangeStatusOnStop();
        }
        private void ShowSendMsgBox(object sender, string arg)
        {
            MessageBox.Show(arg);
        }
        private void OnProduceClick(object sender, RoutedEventArgs arg)
        {
            ChangeStatusOnProduce();
            int cntMax = int.Parse(cnt_);
            int interval = int.Parse(interval_);
            BrokerAddr addr = new BrokerAddr(broker_);
            try
            {
                Utils.TcpPortTest(addr.ip_, addr.port_, 3000);
            }
            catch (Exception ex)
            {
                ChangeStatusOnStop();
                MessageBox.Show(ex.Message);
                return;
            }
            m_cts = new CancellationTokenSource();
            Task.Run(() =>
            {
                int okayCnt = 0;
                int errCnt = 0;
                int sendCnt = 0;
                HashSet<string> errSet = new HashSet<string>();
                Action<DeliveryReport<Null, string>> handler = new Action<DeliveryReport<Null, string>>((r) =>
                {
                    if (r.Error.IsError)
                    { errCnt++; errSet.Add(r.Error.Reason); }
                    else
                        okayCnt++;
                });
                var config = new ProducerConfig
                {
                    BootstrapServers = addr.broker_,
                    MessageTimeoutMs = 3000,

                };

                using (var p = new ProducerBuilder<Null, string>(config).Build())
                {
                    for (int i = 0; i < cntMax; i++)
                    {
                        p.Produce(topic_, new Message<Null, string> { Value = msg_ }, handler);
                        sendCnt++;
                        if (interval > 0)
                            Thread.Sleep(interval);
                        if (m_cts.IsCancellationRequested)
                            break;
                    }
                    p.Flush(m_cts.Token);
                }
                int recvCnt = okayCnt + errCnt;
                string retMsg = "send/recv/error:" + sendCnt + "/" + recvCnt + "/" + errCnt;
                if (errCnt > 0)
                {
                    foreach (var ele in errSet)
                        retMsg += "," + ele;
                }
                OnProduceCompleted?.Invoke(this, retMsg);
                ChangeStatusOnStop();
            });
            
        }
        private void OnStopClick(object sender, RoutedEventArgs arg)
        {
            m_cts.Cancel();
        }
        private void ChangeStatus(bool b)
        {
            btnStop_.IsEnabled = !b;
            btnProduce_.IsEnabled = b;
            txtBroker_.IsEnabled = b;
            txtTopic_.IsEnabled = b;
            txtCnt_.IsEnabled = b;
            txtInterval_.IsEnabled = b;
            txtMsg_.IsEnabled = b;
        }
        private void ChangeStatusOnProduce()
        {
            btnProduce_.Dispatcher.Invoke(()=> { ChangeStatus(false); });
        }
        private void ChangeStatusOnStop()
        {
            btnProduce_.Dispatcher.Invoke(() => { ChangeStatus(true); });
        }
        private CancellationTokenSource m_cts;
    }
}
