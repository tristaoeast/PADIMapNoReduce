using PADIMapNoReduceLibs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading.Tasks;

namespace JobTracker
{
    class JobTracker
    {
        long sentBytes = 0;
        int sentSplits = 0;
        long fileSize = 0;
        int nSplits = 0;
        long finalSizeSplit = 0;
        IDictionary<int, string> workersRegistry = new Dictionary<int, string>();
        String clientURL;

        public delegate int RemoteAsyncDelegateSubmitJobToWorker(long start, long end, int split, String clientURL);
        public delegate void DelegateWorkToWorker(int id);

        static void Main(string[] args)
        {

            if (args.Length < 1)
            {
                Console.WriteLine("ERROR: Wrong number of arguments. Expected format: JOBTRACKER <JOBTRACKER-URL> ");
                Console.WriteLine("Press any key to exit.");
                Console.ReadLine();
                return;
            }

            string[] split1 = args[0].Split(':');
            string[] split2 = split1[2].Split('/');
            int port = Int32.Parse(split2[0]);
            TcpChannel channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(channel, true);

            JobTracker jt = new JobTracker();
            //Activation
            JobTrackerServices jts = new JobTrackerServices(jt);
            RemotingServices.Marshal(jts, "JT", typeof(JobTrackerServices));

            Console.WriteLine("Press any key to exit");
            Console.ReadLine();
        }

        public IList<int> GetSplitRange()
        {
            IList<int> splitsRange = new List<int>();
            //inicio, fim e numero do split

            return splitsRange;
        }

        public void NewSubmitJob(long fileSize, int splits, String className, byte[] code, String clientURL)
        {
            decimal sizeSplit = fileSize / splits;
            finalSizeSplit = (int)System.Math.Round(sizeSplit);
            this.clientURL = clientURL;

            foreach (KeyValuePair<int, string> kvp in workersRegistry)
            {
                SubmitJobToWorker(sentBytes, sentBytes + finalSizeSplit, sentSplits + 1, clientURL, kvp.Key);
                sentSplits++;
                sentBytes += finalSizeSplit + 1;
            }

            //ver o número de workers disponíveis e para cara enviar o um split
            //ex
            //int nWorkers = 2;

            ////TALVEZ VERIFICAR ANTES SE EXISTEM MENOS WORKER QUE SPLITS!!
            //for (int i = 0; i < nWorkers; i++)
            //{
            //    //enviar 1 split a cada worker
            //    SubmitJobToWorker(sentBytes, sentBytes + finalSizeSplit, sentSplits + 1, clientURL, i);
            //    sentSplits++;
            //    sentBytes += finalSizeSplit + 1;
            //}
        }
        public void SubmitJobToWorker(long start, long end, int split, String clientURL, int idWorker)
        {
            //conforme o id ir ver qual o URL desse worker e meter aqui em baixo!!!!!
            IWorker newWorker = (IWorker)Activator.GetObject(typeof(IWorker), workersRegistry[idWorker]);

            AsyncCallback asyncCallback = new AsyncCallback(this.CallBack);
            JobTracker.RemoteAsyncDelegateSubmitJobToWorker remoteDel = new JobTracker.RemoteAsyncDelegateSubmitJobToWorker(newWorker.SubmitJobToWorker);
            remoteDel.BeginInvoke(start, end, split, clientURL, null, null);
        }

        private void CallBack(IAsyncResult ar)
        {
            RemoteAsyncDelegateSubmitJobToWorker rad = (RemoteAsyncDelegateSubmitJobToWorker)((AsyncResult)ar).AsyncDelegate;
            int id = (int)rad.EndInvoke(ar);
            DelegateWorkToWorker delegateWorkToWorker = ManageWorkToWorker;
            delegateWorkToWorker(id);
            //this.Invoke(new DelegateWorkToWorker(this.ManageWorkToWorker), new object[] { id });
        }

        private void ManageWorkToWorker(int id)
        {
            if (sentSplits >= nSplits)
            {
                //TRABALHO TODO FEITO E AVISAR WORKER QUANDO PEDIREM MAIS TRABALHO
                //afinal já não deve ser preciso para para já fica aqui :)
            }
            else
            {
                long end = sentBytes + finalSizeSplit;
                if (end > fileSize)
                {
                    //long newEnd = (fileSize - sentBytes) + sentBytes;
                    //os bytes foram todos enviados! logo
                    SubmitJobToWorker(sentBytes, fileSize, sentSplits + 1, this.clientURL, id);
                    sentBytes = fileSize;
                    sentSplits++;

                }
                else
                {
                    SubmitJobToWorker(sentBytes, end, sentSplits + 1, this.clientURL, id);
                    sentBytes += finalSizeSplit + 1;
                    sentSplits++;
                }
            }
        }

        public void RegisterWorker(int id, string url)
        {
            workersRegistry.Add(id, url);
        }

        public long getSentBytes()
        {
            return sentBytes;
        }

        public void setSentBytes(long newSentBytes)
        {
            sentBytes = newSentBytes;
        }

        public long getFileSize()
        {
            return fileSize;
        }

        public int getSentSplits()
        {
            return sentSplits;
        }

        public void setSentSplits(int newSentSplits)
        {
            sentSplits = newSentSplits;
        }

        public int getNSplits()
        {
            return nSplits;
        }

        public void setNSplits(int setNSplits)
        {
            nSplits = setNSplits;
        }
    }

    public class JobTrackerServices : MarshalByRefObject, IJobTracker
    {
        public JobTracker jobTracker;

        public JobTrackerServices(JobTracker jt)
        {
            jobTracker = jt;
        }

        IList<int> GetSplitRange()
        {
            return jobTracker.GetSplitRange();
        }

        void SubmitJob(long fileSize, int splits, String className, byte[] code, String clientURL)
        {
            jobTracker.NewSubmitJob(fileSize, splits, className, code, clientURL);
        }
    }

    //public class JobTrackerServicesToClient : MarshalByRefObject, IJobTrackerC
    //{
    //    public static JobTracker jobTracker;

    //    public JobTrackerServicesToClient(JobTracker jt)
    //    {
    //        jobTracker = jt;
    //    }

    //    void submitJob(long fileSize, int splits)
    //    {
    //        //implement
    //    }
    //}
}
