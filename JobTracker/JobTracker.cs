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

        public delegate int RemoteAsyncDelegateSubmitJobToWorker(long start, long end, int split, String clientURL);

        static void Main(string[] args)
        {
            TcpChannel channel = new TcpChannel(40000);
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

            if (sentSplits >= nSplits)
            {
                //TRABALHO TODO FEITO E AVISAR WORKER QUANDO PEDIREM MAIS TRABALHO
            }
            else
            {
                if (sentBytes > fileSize)
                {

                }
                else
                {

                }
            }

            return splitsRange;
        }

        public void NewSubmitJob(long fileSize, int splits, String className, byte[] code) 
        {
            decimal sizeSplit = fileSize / splits;
            finalSizeSplit = (int)System.Math.Round(sizeSplit);
            //ver o número de workers disponíveis e para cara enviar o um split
            //ex
            int nWorkers = 2;

            //TALVEZ VERIFICAR ANTES SE EXISTEM MENOS WORKER QUE SPLITS!!
            for (int i = 0; i < nWorkers; i++)
            {
                //enviar 1 split a cada worker
                SubmitJobToWorker(sentBytes, sentBytes + finalSizeSplit, sentSplits + 1, "URL_CLIENT", i);
                sentSplits++;
                sentBytes += finalSizeSplit;
            }
        }
        public void SubmitJobToWorker(long start, long end, int split, String clientURL, int idWorker)
        {
            //conforme o id ir ver qual o URL desse worker e meter aqui em baixo!!!!!
            IWorker newWorker = (IWorker)Activator.GetObject(typeof(IWorker), "METER_URL_BEM");

            AsyncCallback asyncCallback = new AsyncCallback(this.CallBack);
            JobTracker.RemoteAsyncDelegateSubmitJobToWorker remoteDel = new JobTracker.RemoteAsyncDelegateSubmitJobToWorker(newWorker.SubmitJobToWorker);
            remoteDel.BeginInvoke(start, end, split, "CLIENT URL ENVIAR", null, null);
        }

        private void CallBack(IAsyncResult ar)
        {
            RemoteAsyncDelegateSubmitJobToWorker rad = (RemoteAsyncDelegateSubmitJobToWorker)((AsyncResult)ar).AsyncDelegate;
            int id = (int)rad.EndInvoke(ar);

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

        void SubmitJob(long fileSize, int splits, String className, byte[] code)
        {
            jobTracker.NewSubmitJob(fileSize, splits, className, code);
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
