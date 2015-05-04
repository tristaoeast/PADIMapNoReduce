using JobTrackerClientLib;
using PADIMapNoReduceLibs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;

namespace JobTracker
{
    class JobTracker
    {
        int sentBytes = 0;
        int fileSize = 0;
        int nSplits = 0;
        static void Main(string[] args)
        {
            TcpChannel channel = new TcpChannel(40000);
            ChannelServices.RegisterChannel(channel, true);

            JobTracker jt = new JobTracker();
            //Activation
            JobTrackerServicesToWorker jtsw = new JobTrackerServicesToWorker(jt);
            RemotingServices.Marshal(jtsw, "JT", typeof(JobTrackerServicesToWorker));

            Console.WriteLine("Press any key to exit");
            Console.ReadLine();
        }

        public IList<int> GetSplitRange()
        {
            IList<int> splitsRange = new List<int>();
            //inicio, fim e numero do split

            return splitsRange;
        }

        public int getSentBytes() 
        {
            return sentBytes;
        }

    }

    public class JobTrackerServicesToWorker : MarshalByRefObject, IJobTrackerW
    {
        public JobTracker jobTracker;

        public JobTrackerServicesToWorker(JobTracker jt)
        {
            jobTracker = jt;
        }

        IList<int> GetSplitRange()
        {
            return jobTracker.GetSplitRange();
        }

        void SubmitJob(long fileSize, int splits, String className, byte[] code)

        {
            int sentBytes = jobTracker.getSentBytes();
            if (sentBytes > fileSize)
            {

            }
            else
            { 

            }

            //IWorkerJT newWorker = (IWorkerJT)Activator.GetObject(typeof(IWorkerJT), "METER_URL_BEM");
            //newWorker.SubmitJobToWorker(
            //implement
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
