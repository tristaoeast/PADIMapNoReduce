using PADIMapNoReduceLibs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading.Tasks;

namespace JobTracker
{
    public class JobTracker
    {
        long sentBytes = 0;
        int sentSplits = 0;
        long fileSize = 0;
        int nSplits = 0;
        long finalSizeSplit = 0;
        IDictionary<int, string> workersRegistry = new Dictionary<int, string>();
        String clientURL;
        String jtURL;

        int jobsFinished = 0;

        public delegate int RemoteAsyncDelegateSubmitJobToWorker(long start, long end, int split, String clientURL);
        public delegate void DelegateWorkToWorker(int id);

        static void Main(string[] args)
        {

            if (args.Length < 1)
            {
                Console.WriteLine("ERROR: Wrong number of arguments. Expected format: JOBTRACKER <JOBTRACKER-PORT [50001-59999]> <JOBTRACKER-URL> ");
                Console.WriteLine("Press any key to exit.");
                Console.ReadLine();
                return;
            }

            Console.WriteLine("JOBTRACKER-URL: " + args[1]);

            string[] split1 = args[1].Split(':');
            string[] split2 = split1[2].Split('/');
            int port = Int32.Parse(split2[0]);
            TcpChannel channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(channel, true);

            JobTracker jt = new JobTracker();
            //jt.SetJobTrackerURL(args[1]);
            jt.SetJobTrackerURL("tcp://" + Dns.GetHostName() + ":" + args[0] + "/JT");

            //Activation
            JobTrackerServices jts = new JobTrackerServices(jt);
            RemotingServices.Marshal(jts, "JT", typeof(JobTrackerServices));

            Console.WriteLine("Press any key to exit");
            Console.ReadLine();
        }

        public void SetJobTrackerURL(string url)
        {
            jtURL = url;
        }
        public IList<long> GetSplitRange()
        {
            IList<long> splitsRange = new List<long>();
            //inicio, fim e numero do split

            return splitsRange;
        }

        public void SendMapper(String className, byte[] code, String workerURL)
        {
            Console.WriteLine("Sending mapper with name: " + className + " to worker: " + workerURL);
            IWorker worker = (IWorker)Activator.GetObject(typeof(IWorker), workerURL);
            worker.SendMapper(className, code);
        }

        public void NewSubmitJob(long fileSize, int splits, String className, byte[] code, String clientURL)
        {
            decimal sizeSplit = fileSize / splits;
            finalSizeSplit = (long)System.Math.Round(sizeSplit);
            this.clientURL = clientURL;
            this.nSplits = splits;
            this.fileSize = fileSize;

            //Console.WriteLine("fileSize: " + fileSize + " nSplits: " + nSplits + " splitSize: " + finalSizeSplit);

            foreach (KeyValuePair<int, string> kvp in workersRegistry)
            {
                SendMapper(className, code, kvp.Value);
                //if (sentSplits + 1 == nSplits)
                //{
                //    SubmitJobToWorker(sentBytes, sentBytes + finalSizeSplit * 10, sentSplits + 1, clientURL, kvp.Key);
                //    sentSplits++;
                //    break;
                //}

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
            Console.WriteLine("Submiting job to worker: " + workersRegistry[idWorker]);
            remoteDel.BeginInvoke(start, end, split, clientURL, asyncCallback, null);
        }

        private void CallBack(IAsyncResult ar)
        {
            RemoteAsyncDelegateSubmitJobToWorker rad = (RemoteAsyncDelegateSubmitJobToWorker)((AsyncResult)ar).AsyncDelegate;
            int id = (int)rad.EndInvoke(ar);
            Console.WriteLine("Worker with ID: " + id + "finished his split.");
            DelegateWorkToWorker delegateWorkToWorker = ManageWorkToWorker;
            delegateWorkToWorker(id);
            jobsFinished++;
            if (jobsFinished == nSplits)
            {
                Console.WriteLine("Sending job finished signal to client:" + clientURL);
                IClient client = (IClient)Activator.GetObject(typeof(IClient), clientURL);
                client.notifyJobFinished(true);
                Console.WriteLine("Job finished signal sent");
            }
            //this.Invoke(new DelegateWorkToWorker(this.ManageWorkToWorker), new object[] { id });
        }

        private void ManageWorkToWorker(int id)
        {
            Console.WriteLine("sentSplits: " + sentSplits + " nSplits: " + nSplits);
            if (sentSplits >= nSplits)
            {
                Console.WriteLine("ENTREI222");
                //TRABALHO TODO FEITO E AVISAR WORKER QUANDO PEDIREM MAIS TRABALHO
                //afinal já não deve ser preciso para para já fica aqui :)
            }
            else
            {
                long end = sentBytes + finalSizeSplit;
                Console.WriteLine("sentBytes: " + sentBytes + " splitSize: " + finalSizeSplit + "          end: " + end + " fileSize: " + fileSize + Environment.NewLine);
                if (end >= fileSize)
                {
                    //end += finalSizeSplit * 10;
                    //SubmitJobToWorker(sentBytes, end, sentSplits + 1, this.clientURL, id);
                    //long newEnd = (fileSize - sentBytes) + sentBytes;
                    //os bytes foram todos enviados! logo
                    Console.WriteLine("11111 Submitting new split to worker with start and end <" + id + ", " + sentBytes + ", " + end + ">" + Environment.NewLine);
                    SubmitJobToWorker(sentBytes, fileSize, sentSplits + 1, this.clientURL, id);
                    sentBytes = fileSize;
                    sentSplits++;

                }
                else
                {
                    Console.WriteLine("22222 Submitting new split to worker with start and end <" + id + ", " + sentBytes + ", " + end + ">" + Environment.NewLine);
                    SubmitJobToWorker(sentBytes, end, sentSplits + 1, this.clientURL, id);
                    sentBytes += finalSizeSplit + 1;
                    sentSplits++;
                }
            }
        }

        public void RegisterWorker(int id, string url)
        {
            Console.WriteLine("Registering worker with ID: " + id + "and URL: " + url);
            workersRegistry.Add(id, url);
        }

        public void StatusRequest()
        {
            Console.WriteLine("I'm JobTracker at: " + jtURL + " and I'm alive!");
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

        public IList<long> GetSplitRange()
        {
            return jobTracker.GetSplitRange();
        }

        public void SubmitJob(long fileSize, int splits, String className, byte[] code, String clientURL)
        {
            Console.WriteLine("Submit job received from :" + clientURL);
            jobTracker.NewSubmitJob(fileSize, splits, className, code, clientURL);
        }

        public void RegisterWorker(int id, string url)
        {
            jobTracker.RegisterWorker(id, url);
        }

        public void StatusRequest()
        {
            jobTracker.StatusRequest();
        }
    }
}
