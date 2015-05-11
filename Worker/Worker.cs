using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using PADIMapNoReduceLibs;
using System.Reflection;
using System.IO;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Timers;

namespace Worker
{

    public delegate void RemoteAsyncDelegateSendResultsToClient(IList<KeyValuePair<string, string>> result, int split);
    public delegate void RADSubmitJobToTracker(long fileSize, int splits, String className, byte[] code, String clientURL);
    public delegate void RADRegisterWorker(int id, string url);
    public delegate void RADFreezeUnfreezeJT();
    public delegate void DelegateOutputMessage(string msg);

    class Worker
    {
        String jobTrackerURL = String.Empty;
        String clientURL = String.Empty;
        int myId;
        int port;
        bool freeze = false;
        String status = "Alive";
        System.Timers.Timer aTimer = new System.Timers.Timer(10000);

        static void Main(string[] args)
        {

            Worker w = new Worker();
            Console.WriteLine(args[0]);

            if (args.Length < 2)
            {
                Console.WriteLine("ERROR: Wrong number of arguments. Expected format: WORKER <ID> <SERVICE-URL> [<ENTRY-URL>] ");
                Console.WriteLine("Press any key to exit.");
                Console.ReadLine();
                return;
            }

            string[] split1 = args[1].Split(':');
            string[] split2 = split1[2].Split('/');
            w.setPort(Int32.Parse(split2[0]));

            TcpChannel channel = new TcpChannel(Int32.Parse(split2[0]));
            ChannelServices.RegisterChannel(channel, true);

            //Activation
            WorkerServices workerServices = new WorkerServices(w);
            RemotingServices.Marshal(workerServices, "W", typeof(WorkerServices));

            string entryURL;
            if (args.Length == 2)
            {
                entryURL = args[1];
            }
            else
                entryURL = args[2];

            w.setId(Int32.Parse(args[0]));

            //TODO: IF SERVICE-URL == ENTRY-URL create worker 
            if (args[1].Equals(entryURL, StringComparison.OrdinalIgnoreCase))
            {
                int jtPort = Int32.Parse(split2[0]) + 20000;
                string jtURL = "tcp://" + Dns.GetHostName() + ":" + jtPort + "/JT";
                w.SetJobTrackerURL(jtURL);
                Process.Start(@"..\..\..\JobTracker\bin\Debug\JobTracker.exe", jtPort + " " + jtURL);
            }

            //REGISTER WITH WORKER, WHICH FORWARDS TO JT
            IWorker entryWorker = (IWorker)Activator.GetObject(typeof(IWorker), entryURL);
            RADRegisterWorker remoteDel = new RADRegisterWorker(entryWorker.RegisterWorker);
            Console.WriteLine("Sending registration to: " + entryURL);
            remoteDel.BeginInvoke(Int32.Parse(args[0]), "tcp://" + Dns.GetHostName() + ":" + split2[0] + "/W", null, null);

            w.SetJobTrackerURL(entryWorker.GetJobTrackerURL());

            //Send im alive to job tracker every 10 seconds
            // Hook up the Elapsed event for the timer.    
            w.getTimer().Elapsed += (sender, e) => ImAlive(sender, e, w);
            w.getTimer().Enabled = true;

            Console.WriteLine("Working on thread: " + Thread.CurrentThread.ManagedThreadId);

            System.Console.WriteLine("Press <enter> to terminate worker with ID: " + args[0] + " and URL: " + args[1] + "...");
            System.Console.ReadLine();
        }

        public System.Timers.Timer getTimer()
        {
            return aTimer;
        }

        private static void ImAlive(Object source, ElapsedEventArgs e, Worker w)
        {
            w.handleFreeze();
            Console.WriteLine("Worker {0} sending Im Alive from thread {1}", w.getId(), Thread.CurrentThread.ManagedThreadId);
            IJobTracker jt = (IJobTracker)Activator.GetObject(typeof(IJobTracker), w.GetJobTrackerURL());
            jt.ReceiveImAlive(w.getId());
        }

        public void setPort(int p)
        {
            port = p;
        }

        public void setId(int id)
        {
            myId = id;
        }

        public int getId()
        {
            return myId;
        }

        public void SetJobTrackerURL(string url)
        {
            jobTrackerURL = url;
        }

        public string GetJobTrackerURL()
        {
            return jobTrackerURL;
        }

        public void SetClientURL(string url)
        {
            clientURL = url;
        }

        public void SendResultToClient(IList<KeyValuePair<string, string>> result, int split, string url)
        {
            handleFreeze();
            IClient client = (IClient)Activator.GetObject(typeof(IClient), url);
            RemoteAsyncDelegateSendResultsToClient remoteDel = new RemoteAsyncDelegateSendResultsToClient(client.ReturnResult);
            Console.WriteLine("Sending result of split: " + split + " to client: " + url);
            remoteDel.BeginInvoke(result, split, null, null);
        }

        public void SubmitJobToTracker(long fileSize, int splits, String className, byte[] code, String clientURL)
        {
            Console.WriteLine("Received SubmitJob from: " + clientURL + ". Forwarding to: " + jobTrackerURL);
            IJobTracker jobTracker = (IJobTracker)Activator.GetObject(typeof(IJobTracker), jobTrackerURL);
            RADSubmitJobToTracker remoteDel = new RADSubmitJobToTracker(jobTracker.SubmitJob);
            remoteDel.BeginInvoke(fileSize, splits, className, code, clientURL, null, null);
        }

        public void RegisterWorker(int id, string url)
        {
            Console.WriteLine("Sending RegisterWorker of ID: " + id + " with URL: " + url + " to JobTracker at " + jobTrackerURL);
            IJobTracker jobTracker = (IJobTracker)Activator.GetObject(typeof(IJobTracker), jobTrackerURL);
            RADRegisterWorker remoteDel = new RADRegisterWorker(jobTracker.RegisterWorker);
            remoteDel.BeginInvoke(id, url, null, null);
        }
        public void StatusRequest()
        {
            Console.WriteLine("Worker " + getId() + " STATUS: " + status);
        }
        public void Freeze(bool jt)
        {
            //TODO: 
            //se jt for false manda dormir o worker
            if (!jt)
            {
                freeze = true;
                status = "Frozen";
                Console.WriteLine("Freezing Worker " + getId() + " now");
            }
            //se jt for true manda dormir so o jobtracker
            else
            {
                IJobTracker jobTracker = (IJobTracker)Activator.GetObject(typeof(IJobTracker), jobTrackerURL);
                RADFreezeUnfreezeJT remoteFreezeUnfreeze = new RADFreezeUnfreezeJT(jobTracker.Freeze);
                remoteFreezeUnfreeze.BeginInvoke(null, null);
            }
        }

        public void Unfreeze(bool jt)
        {
            //TODO: 
            //se jt for false manda acordar o worker
            if (!jt)
            {
                freeze = false;
                status = "Alive";
                Console.WriteLine("Unfreezing Worker " + getId() + " now");
            }
            //se jt for true manda acordar o jobtracker
            else
            {
                IJobTracker jobTracker = (IJobTracker)Activator.GetObject(typeof(IJobTracker), jobTrackerURL);
                RADFreezeUnfreezeJT remoteFreezeUnfreeze = new RADFreezeUnfreezeJT(jobTracker.Unfreeze);
                remoteFreezeUnfreeze.BeginInvoke(null, null);
            }
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

    public delegate void DelRegisterWorker(int id, string url);

    class WorkerServices : MarshalByRefObject, IWorker
    {
        Worker worker;
        object mapObject = null;
        Type mapType;

        System.Threading.Thread t = null;

        public WorkerServices(Worker w)
        {
            worker = w;
        }

        public bool SendMapper(String className, byte[] code)
        {

            worker.handleFreeze();
            Assembly assembly = Assembly.Load(code);

            // Walk through each type in the assembly looking for our class
            foreach (Type type in assembly.GetTypes())
            {
                //Console.WriteLine("Type: " + type.FullName);
                if (type.IsClass == true)
                {
                    if (type.FullName.EndsWith("." + className))
                    {
                        mapType = type;
                        // create an instance of the object
                        mapObject = Activator.CreateInstance(type);
                        Console.WriteLine("MapObject loadded successfully");
                        return true;

                    }
                }
            }
            Console.WriteLine("Could not invoke method");
            throw (new System.Exception("could not invoke method"));
        }

        public int SubmitJobToWorker(long start, long end, int split, string clientURL)
        {
            worker.handleFreeze();

            t = System.Threading.Thread.CurrentThread;

            Console.WriteLine("Job submitted starting on: " + start + " and ending on: " + end + " running on thread " + Thread.CurrentThread.ManagedThreadId);
            worker.SetClientURL(clientURL);
            Console.WriteLine("1");
            IList<KeyValuePair<String, String>> result = new List<KeyValuePair<String, String>>();
            Console.WriteLine("2");
            // Client URL must be something like "tcp://localhost:10001/C"
            IClient client = (IClient)Activator.GetObject(typeof(IClient), clientURL);
            Console.WriteLine("3");
            byte[] fileSplitByte = client.GetSplit(start, end);
            Console.WriteLine("4");
            String filePath = split.ToString() + ".in";
            Console.WriteLine("5");
            File.WriteAllBytes(filePath, fileSplitByte);
            Console.WriteLine("6");

            // Read each line from the string
            System.IO.StreamReader file = new System.IO.StreamReader(filePath);
            Console.WriteLine("7");
            string line;
            Console.WriteLine("8");
            while (mapObject == null)
            {
                Console.WriteLine("Waiting for map Object...");
            }
            while ((line = file.ReadLine()) != null)
            {
                // Dynamically Invoke the method
                object[] args = new object[] { line };
                object resultObject = mapType.InvokeMember("Map",
                  BindingFlags.Default | BindingFlags.InvokeMethod,
                       null,
                       mapObject,
                       args);
                foreach (KeyValuePair<string, string> kvp in (IList<KeyValuePair<string, string>>)resultObject)
                {
                    result.Add(kvp);
                }
            }
            //using (System.IO.StreamWriter outFile = new System.IO.StreamWriter(split.ToString() + ".out"))
            //{
            //    foreach (var l in result)
            //    {
            //        outFile.WriteLine("<" + l.Key + ", " + l.Value + ">");
            //    }
            //}
            worker.SendResultToClient(result, split, clientURL);
            Console.WriteLine("Result of split: " + split + " to client: " + clientURL + " sent.");
            worker.handleFreeze();
            return worker.getId();
        }

        public void SubmitJobToTracker(long fileSize, int splits, String className, byte[] code, String clientURL)
        {

            worker.handleFreeze();
            worker.SubmitJobToTracker(fileSize, splits, className, code, clientURL);
        }

        public void RegisterWorker(int id, string url)
        {

            worker.handleFreeze();
            worker.RegisterWorker(id, url);
        }

        public void StatusRequest()
        {
            Console.WriteLine("Trying to get status...");
            worker.StatusRequest();
        }

        public void Freeze(bool jt)
        {
            worker.handleFreeze();
            Console.WriteLine("Trying to freeze...");
            worker.Freeze(jt);
        }

        public void Unfreeze(bool jt)
        {
            Console.WriteLine("Trying to Unfreeze...");
            worker.Unfreeze(jt);
        }

        public String GetJobTrackerURL()
        {
            return worker.GetJobTrackerURL();
        }

        public void Slow(int secs)
        {
            Console.WriteLine("[{2}]Slowing worker {0} for {1} seconds .", worker.getId(), secs, Thread.CurrentThread.ManagedThreadId);

            Console.WriteLine("1");
            t.Suspend();
            worker.getTimer().Enabled = false;
            Console.WriteLine("2");
            Thread.Sleep(secs * 1000);
            Console.WriteLine("3");
            worker.getTimer().Enabled = true;
            t.Resume();
            Console.WriteLine("4");

        }
    }
}
