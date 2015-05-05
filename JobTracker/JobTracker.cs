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
        long sentBytes = 0;
        int sentSplits = 0;
        long fileSize = 0;
        int nSplits = 0;
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

            return splitsRange;
        }

        public long getSentBytes() 
        {
            return sentBytes;
        }

        public long getFileSize()
        {
            return fileSize;
        }

        public int getSentSplits() 
        {
            return sentSplits;
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
            //implementarrrrr
            long sentBytes = jobTracker.getSentBytes();
            if (sentBytes > jobTracker.getFileSize())
            {

            }
            else
            {

            }
            return jobTracker.GetSplitRange();
        }

        void SubmitJob(long fileSize, int splits, String className, byte[] code)

        {
             
            decimal a = fileSize/splits;
            long b = (int) System.Math.Round(a);
            //ver o número de workers disponíveis e para cara enviar o um split
            //ex
            int nWorkers = 2;

            long c = 0;
            int sentSplits = jobTracker.getSentSplits();
            for (int i = 0; i < nWorkers; i++)
            {
                //enviar 1 split a cada worker
                IWorker newWorker = (IWorker)Activator.GetObject(typeof(IWorker), "METER_URL_BEM");
                newWorker.SubmitJobToWorker(c, c+b, sentSplits);

                sentSplits++;
                c += b;
            }

            //set dos sent bytes e splits
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
