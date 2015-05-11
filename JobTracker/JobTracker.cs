using PADIMapNoReduceLibs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

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
        bool freeze = false;
        
        int jobsFinished = 0;

        bool jobFinished = true;

        Queue<int> unfinishedSplitsQ = new Queue<int>();
        IDictionary<int, long> splitStart = new Dictionary<int, long>();
        IDictionary<int, long> splitEnd = new Dictionary<int, long>();
        IDictionary<int, int> workerCurrentSplit = new Dictionary<int, int>();
        IDictionary<int, bool> workerAlive = new Dictionary<int, bool>();
        //IDictionary<int, bool> workerOnline = new Dictionary<int, bool>();



        public delegate int RemoteAsyncDelegateSubmitJobToWorker(long start, long end, int split, String clientURL);
        public delegate bool RADSendMapper(String className, byte[] code);
        public delegate void DelegateWorkToWorker(int id);
        public delegate void DelegateEnqueueSplit(int split, long start, long end, int idWorker);
        public delegate void DelegateOutputMessage(string msg);

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

            //Check every 20 seconds if workers are alive
            System.Timers.Timer aTimer = new System.Timers.Timer(20000);
            // Hook up the Elapsed event for the timer. 
            aTimer.Elapsed += (sender, e) => CheckWorkersAlive(sender, e, jt);
            aTimer.Enabled = true;


            Console.WriteLine("Press any key to exit");
            Console.ReadLine();
        }

        private static void CheckWorkersAlive(Object source, ElapsedEventArgs e, JobTracker jt)
        {
            Console.WriteLine("Checking workers alive...");
            IDictionary<int, bool> workerAlive = jt.getWorkerAlive();
            if (!jt.IsJobFinished())
            {
                foreach (KeyValuePair<int, bool> kvp in workerAlive)
                {
                    Console.WriteLine("Worker {0} has value {1}", kvp.Key,kvp.Value);
                    if (!workerAlive[kvp.Key])
                    {
                        int split = jt.getWorkerCurrentSplit(kvp.Key);
                        if (split != 0)
                        {
                            jt.SetWorkerCurrentSplit(kvp.Key, 0);
                            long start = jt.getSplitStart(kvp.Key);
                            long end = jt.getSplitEnd(kvp.Key);
                            DelegateEnqueueSplit des = jt.EnqueueSplit;
                            des(split, start, end, kvp.Key);
                            DelegateOutputMessage dom = jt.dbg;
                            dom("Worker " + kvp.Key + " unresponsive. Enqueueing split with start and end <" + split + ", " + start + ", " + end + ">");
                        }
                    }
                    else
                    {
                        jt.SetWorkerAlive(kvp.Key, false);
                    }
                }
            }

        }

        public bool IsJobFinished()
        {
            return jobFinished;
        }

        public IDictionary<int, bool> getWorkerAlive()
        {
            return workerAlive;
        }

        public void SetWorkerAlive(int id, bool alive)
        {
            if (workerAlive.ContainsKey(id))
                workerAlive.Remove(id);
            workerAlive.Add(id, alive);
        }

        public void SetWorkerCurrentSplit(int id, int split)
        {
            if (workerCurrentSplit.ContainsKey(id))
                workerCurrentSplit.Remove(id);
            workerCurrentSplit.Add(id, split);
        }

        public int getWorkerCurrentSplit(int id)
        {
            if (workerCurrentSplit.ContainsKey(id))
                return workerCurrentSplit[id];
            return 0;
        }

        public long getSplitStart(int id)
        {
            if (splitStart.ContainsKey(id))
                return splitStart[id];
            return 0;
        }

        public long getSplitEnd(int id)
        {
            if (splitEnd.ContainsKey(id))
                return splitEnd[id];
            return 0;
        }

        public void dbg(string s)
        {
            Console.WriteLine(s);
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

        public void EnqueueSplit(int split, long start, long end, int idWorker)
        {
            splitStart.Remove(split);
            splitStart.Add(split, start);
            splitEnd.Remove(split);
            splitEnd.Add(split, end);
            unfinishedSplitsQ.Enqueue(split);
            workerAlive.Remove(idWorker);
            workerAlive.Add(idWorker, false);
        }

        public bool SendMapper(String className, byte[] code, String workerURL, long start, long end, int split, int idWorker)
        {
            IWorker worker = (IWorker)Activator.GetObject(typeof(IWorker), workerURL);

            try
            {
                //RADSendMapper remDel = new RADSendMapper(worker.SendMapper);
                //remDel.BeginInvoke(className, code, null, null);
                worker.SendMapper(className, code);
                Console.WriteLine("Sending mapper with name: " + className + " to worker: " + workerURL);
                return true;
            }
            catch (Exception)
            {
                //splitStart.Remove(split);
                //splitStart.Add(split, start);
                //splitEnd.Remove(split);
                //splitEnd.Add(split, end);
                //Console.WriteLine("SEND MAPPER: Could not locate worker at: " + workersRegistry[idWorker] + ". Enqueueing split {0} with start {1} and end {2}", split, start, end);
                Console.WriteLine("SEND MAPPER: Could not locate worker at: " + workersRegistry[idWorker]);
                //unfinishedSplitsQ.Enqueue(split);
                if (workerCurrentSplit.ContainsKey(idWorker))
                    workerCurrentSplit.Remove(idWorker);
                workerCurrentSplit.Add(idWorker, 0);
                if (workerAlive.ContainsKey(idWorker))
                    workerAlive.Remove(idWorker);
                workerAlive.Add(idWorker, false);
                return false;
            }
        }

        public void NewSubmitJob(long fileSize, int splits, String className, byte[] code, String clientURL)
        {
            jobFinished = false;
            decimal sizeSplit = fileSize / splits;
            finalSizeSplit = (long)System.Math.Round(sizeSplit);
            this.clientURL = clientURL;
            this.nSplits = splits;
            this.fileSize = fileSize;

            //Console.WriteLine("fileSize: " + fileSize + " nSplits: " + nSplits + " splitSize: " + finalSizeSplit);

            foreach (KeyValuePair<int, string> kvp in workersRegistry)
            {
                bool mapSent = SendMapper(className, code, kvp.Value, sentBytes, sentBytes + finalSizeSplit, sentSplits + 1, kvp.Key);
                //if (sentSplits + 1 == nSplits)
                //{
                //    SubmitJobToWorker(sentBytes, sentBytes + finalSizeSplit * 10, sentSplits + 1, clientURL, kvp.Key);
                //    sentSplits++;
                //    break;
                //}

                if (mapSent)
                {
                    splitStart.Remove(sentSplits + 1);
                    splitStart.Add(sentSplits + 1, sentBytes);
                    splitEnd.Remove(sentSplits + 1);
                    splitEnd.Add(sentSplits + 1, sentBytes + finalSizeSplit);
                    SubmitJobToWorker(sentBytes, sentBytes + finalSizeSplit, sentSplits + 1, clientURL, kvp.Key);
                    sentSplits++;
                    sentBytes += finalSizeSplit + 1;
                }

            }
        }

        public void SubmitJobToWorker(long start, long end, int split, String clientURL, int idWorker)
        {

            //conforme o id ir ver qual o URL desse worker e meter aqui em baixo!!!!!
            IWorker newWorker = (IWorker)Activator.GetObject(typeof(IWorker), workersRegistry[idWorker]);

            AsyncCallback asyncCallback = new AsyncCallback(this.SJTWCallBack);
            JobTracker.RemoteAsyncDelegateSubmitJobToWorker remoteDel = new JobTracker.RemoteAsyncDelegateSubmitJobToWorker(newWorker.SubmitJobToWorker);
            Console.WriteLine("Submiting job to worker: " + workersRegistry[idWorker]);
            if (workerCurrentSplit.ContainsKey(idWorker))
                workerCurrentSplit.Remove(idWorker);
            workerCurrentSplit.Add(idWorker, split);
            remoteDel.BeginInvoke(start, end, split, clientURL, asyncCallback, new object[] { split, start, end, idWorker });

        }

        private void SJTWCallBack(IAsyncResult ar)
        {
            RemoteAsyncDelegateSubmitJobToWorker rad = (RemoteAsyncDelegateSubmitJobToWorker)((AsyncResult)ar).AsyncDelegate;
            //int id = (int)rad.EndInvoke(ar);
            //Console.WriteLine("Worker with ID: " + id + "finished his split.");
            //workerAlive.Remove(id);
            //workerAlive.Add(id, true);
            //DelegateWorkToWorker delegateWorkToWorker = ManageWorkToWorker;
            //delegateWorkToWorker(id);
            //jobsFinished++;
            //if (jobsFinished == nSplits)
            //{
            //    Console.WriteLine("Sending job finished signal to client:" + clientURL);
            //    IClient client = (IClient)Activator.GetObject(typeof(IClient), clientURL);
            //    client.notifyJobFinished(true);
            //    Console.WriteLine("Job finished signal sent");
            //}
            try
            {
                int id = (int)rad.EndInvoke(ar);
                Console.WriteLine("Worker with ID: " + id + "finished his split.");
                workerAlive.Remove(id);
                workerAlive.Add(id, true);
                DelegateWorkToWorker delegateWorkToWorker = ManageWorkToWorker;
                delegateWorkToWorker(id);
                jobsFinished++;
                if (jobsFinished == nSplits)
                {
                    jobFinished = true;
                    Console.WriteLine("Sending job finished signal to client:" + clientURL);
                    IClient client = (IClient)Activator.GetObject(typeof(IClient), clientURL);
                    client.notifyJobFinished(true);
                    Console.WriteLine("Job finished signal sent");
                }
                //this.Invoke(new DelegateWorkToWorker(this.ManageWorkToWorker), new object[] { id });
            }
            catch (SocketException)
            {
                object[] state = (object[])ar.AsyncState;
                int split = (int)state[0];
                long start = (long)state[1];
                long end = (long)state[2];
                int idWorker = (int)state[3];
                DelegateEnqueueSplit des = this.EnqueueSplit;
                des(split, start, end, idWorker);
                DelegateOutputMessage dom = this.dbg;
                dom("SJTW CALL BACK: Could not locate worker at: " + workersRegistry[idWorker] + ". Enqueueing split " + split + " with start " + start + " and end " + end);
                return;
            }
        }

        private void ManageWorkToWorker(int id)
        {
            Console.WriteLine("sentSplits: " + sentSplits + " nSplits: " + nSplits);

            if (unfinishedSplitsQ.Count > 0)
            {
                int split = unfinishedSplitsQ.Dequeue();
                Console.WriteLine("There's a job queued. It is split number: " + split);
                long start = splitStart[split];
                long end = splitEnd[split];
                Console.WriteLine("Submitting queued split to worker with start and end <" + split + ", " + id + ", " + start + ", " + end + ">" + Environment.NewLine);
                SubmitJobToWorker(start, end, split, this.clientURL, id);
            }
            else if (sentSplits >= nSplits)
            {

                while (!jobFinished)
                {
                    if (unfinishedSplitsQ.Count > 0)
                    {
                        int split = unfinishedSplitsQ.Dequeue();
                        Console.WriteLine("There's a job queued. It is split number: " + split);
                        long start = splitStart[split];
                        long end = splitEnd[split];
                        Console.WriteLine("Submitting queued split to worker with start and end <" + split + ", " + id + ", " + start + ", " + end + ">" + Environment.NewLine);
                        SubmitJobToWorker(start, end, split, this.clientURL, id);
                    }
                    else
                    {
                        Console.WriteLine("All splits sent but job isn't finished and queue is empty. Checking again in 3 seconds");
                        Thread.Sleep(3000);
                    }
                }

                Console.WriteLine("ENTREI222");
                //TRABALHO TODO FEITO E AVISAR WORKER QUANDO PEDIREM MAIS TRABALHO
            }//TODO: verify queue for unresolved messages... CHECK
            else
            {
                long end = sentBytes + finalSizeSplit;
                //Console.WriteLine("sentBytes: " + sentBytes + " splitSize: " + finalSizeSplit + "          end: " + end + " fileSize: " + fileSize + Environment.NewLine);
                if (end >= fileSize)
                {
                    //end += finalSizeSplit * 10;
                    //SubmitJobToWorker(sentBytes, end, sentSplits + 1, this.clientURL, id);
                    //long newEnd = (fileSize - sentBytes) + sentBytes;
                    //os bytes foram todos enviados! logo
                    Console.WriteLine("Submitting new split to worker with start and end <" + id + ", " + sentBytes + ", " + end + ">" + Environment.NewLine);
                    SubmitJobToWorker(sentBytes, fileSize, sentSplits + 1, this.clientURL, id);
                    sentBytes = fileSize;
                    sentSplits++;

                }
                else
                {
                    Console.WriteLine("Submitting new split to worker with start and end <" + id + ", " + sentBytes + ", " + end + ">" + Environment.NewLine);
                    SubmitJobToWorker(sentBytes, end, sentSplits + 1, this.clientURL, id);
                    sentBytes += finalSizeSplit + 1;
                    sentSplits++;
                }
            }
        }

        public void RegisterWorker(int id, string url)
        {
            Console.WriteLine("Registering worker with ID: " + id + "and URL: " + url);
            workerAlive.Add(id, true);
            workersRegistry.Add(id, url);
        }

        public void StatusRequest()
        {
            Console.WriteLine("I'm JobTracker " + jtURL + " and I'm in charge");
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

        public void Freeze()
        {
            Console.WriteLine("Freezing JobTracker now");
            freeze = true;
        }

        public void Unfreeze()
        {
            Console.WriteLine("Unfreezing JobTracker now");
            freeze = false;
        }

        public void handleFreeze()
        {
            lock (this)
            {
                if (freeze)
                {
                    Monitor.Wait(this);
                }
            }
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
            jobTracker.handleFreeze();
            return jobTracker.GetSplitRange();
        }

        public void SubmitJob(long fileSize, int splits, String className, byte[] code, String clientURL)
        {
            jobTracker.handleFreeze();
            Console.WriteLine("Submit job received from :" + clientURL);
            jobTracker.NewSubmitJob(fileSize, splits, className, code, clientURL);
        }

        public void RegisterWorker(int id, string url)
        {
            jobTracker.handleFreeze();
            jobTracker.RegisterWorker(id, url);
        }

        public void StatusRequest()
        {
            jobTracker.handleFreeze();
            jobTracker.StatusRequest();
        }

        public void Freeze()
        {
            jobTracker.handleFreeze();
            Console.WriteLine("Trying to freeze...");
            jobTracker.Freeze();
        }

        public void Unfreeze()
        {
            Console.WriteLine("Trying to Unfreeze...");
            jobTracker.Unfreeze();
        }

        public void ReceiveImAlive(int id)
        {
            Console.WriteLine("Got an Im Alive from worker" + id);
            jobTracker.SetWorkerAlive(id, true);
        }
    }
}
