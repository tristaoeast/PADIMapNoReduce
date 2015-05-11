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
        public delegate void DelegateSetWorkerAlive(int id, bool alive);

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

            Console.WriteLine("Starint timer on thread {0}...", Thread.CurrentThread.ManagedThreadId);
            //Check every 20 seconds if workers are alive
            System.Timers.Timer aTimer = new System.Timers.Timer(20000);
            // Hook up the Elapsed event for the timer. 
            aTimer.Elapsed += jt.CheckWorkersAlive;
            aTimer.Enabled = true;


            Console.WriteLine("Press any key to exit");
            Console.ReadLine();
        }

        private void CheckWorkersAlive(Object source, ElapsedEventArgs e)
        {
            Console.WriteLine("Checking workers alive from thread {0} ...", Thread.CurrentThread.ManagedThreadId);
            if (!IsJobFinished())
            {
                List<int> keys = new List<int>(getWorkerAlive().Keys);
                foreach (int key in keys)
                {
                    Console.WriteLine("Worker {0} has value {1}", key, getWorkerAlive()[key]);
                    if (!getWorkerAlive()[key])
                    {
                        Console.WriteLine("Worker " + key + " unresponsive. ");
                        int split = getWorkerCurrentSplit(key);
                        if (split != 0)
                        {
                            Console.WriteLine("1");
                            SetWorkerCurrentSplit(key, 0);
                            Console.WriteLine("2");
                            long start = getSplitStart(key);
                            Console.WriteLine("3");
                            long end = getSplitEnd(key);
                            Console.WriteLine("4");
                            DelegateEnqueueSplit des = EnqueueSplit;
                            Console.WriteLine("5");
                            des(split, start, end, key);
                            Console.WriteLine("6");
                            DelegateOutputMessage dom = dbg;
                            dom("Enqueueing split with start and end <" + split + ", " + start + ", " + end + ">");
                        }
                    }
                    else
                    {
                        getWorkerAlive()[key] = false;
                        Console.WriteLine("Set worker {0} alive to false", key);
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
            Console.WriteLine("Set worker {0} alive {1} from thread {2}", id, alive, Thread.CurrentThread.ManagedThreadId);
            if (workerAlive.ContainsKey(id))
                workerAlive[id] = alive;
            else
                workerAlive.Add(id, alive);

        }

        public void SetWorkerCurrentSplit(int id, int split)
        {
            if (workerCurrentSplit.ContainsKey(id))
                workerCurrentSplit[id] = split;
            else
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
            if (splitStart.ContainsKey(split))
                splitStart[split] = start;
            else
                splitStart.Add(split, start);

            if (splitEnd.ContainsKey(split))
                splitEnd[split] = end;
            else
                splitEnd.Add(split, end);

            unfinishedSplitsQ.Enqueue(split);

            SetWorkerAlive(idWorker, false);

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
                else
                    workerCurrentSplit.Add(idWorker, 0);
                if (workerAlive.ContainsKey(idWorker))
                    workerAlive[idWorker] = false;
                else
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
                    if (splitStart.ContainsKey(sentSplits + 1))
                        splitStart.Remove(sentSplits + 1);
                    else
                        splitStart.Add(sentSplits + 1, sentBytes);
                    if (splitEnd.ContainsKey(sentSplits + 1))
                        splitEnd.Remove(sentSplits + 1);
                    else
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
            else
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
                if (workerAlive.ContainsKey(id))
                    workerAlive[id] = true;
                else 
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
            if (workerAlive.ContainsKey(id))
                workerAlive[id] = true;
            else
                workerAlive.Add(id, true);
            if (workersRegistry.ContainsKey(id))
                workersRegistry[id] = url;
            else
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
