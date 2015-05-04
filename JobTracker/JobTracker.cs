using JobTrackerClientLib;
using JobTrackerWorkerLib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting;
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
            JobTracker jt = new JobTracker();
            //Activation
            JobTrackerServicesToWorker jtsw = new JobTrackerServicesToWorker(jt);
            RemotingServices.Marshal(jtsw, "JT", typeof(JobTrackerServicesToWorker));

            Console.WriteLine("Press any key to exit");
            Console.ReadLine();
        }

        public IList<int> getSplitRange()
        {
            IList<int> splitsRange = new List<int>();
            //inicio, fim e numero do split

            return splitsRange;
        }

    }

    public class JobTrackerServicesToWorker : MarshalByRefObject, IJobTrackerW
    {
        public JobTracker jobTracker;

        public JobTrackerServicesToWorker(JobTracker jt)
        {
            jobTracker = jt;
        }

        IList<int> getSplitRange()
        {
            return jobTracker.getSplitRange();
        }

        void submitJob(long fileSize, int splits)
        {
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
