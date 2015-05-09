﻿using System;
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

namespace Worker
{

    public delegate void RemoteAsyncDelegateSendResultsToClient(IList<KeyValuePair<string, string>> result, int split);
    public delegate void RADSubmitJobToTracker(long fileSize, int splits, String className, byte[] code, String clientURL);
    public delegate void RADRegisterWorker(int id, string url);
    public delegate void RADRequestJTStatus();

    class Worker
    {
        String jobTrackerURL = String.Empty;
        String clientURL = String.Empty;
        int myId;


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
            int port = Int32.Parse(split2[0]);

            TcpChannel channel = new TcpChannel(port);
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
                int jtPort = port + 20000;
                string jtURL = "tcp://" + Dns.GetHostName() + ":" + jtPort + "/JT";
                w.SetJobTrackerURL(jtURL);
                Process.Start(@"..\..\..\JobTracker\bin\Debug\JobTracker.exe", jtPort + " " + jtURL);
            }

            //REGISTER WITH WORKER, WHICH FORWARDS TO JT
            IWorker entryWorker = (IWorker)Activator.GetObject(typeof(IWorker), entryURL);
            RADRegisterWorker remoteDel = new RADRegisterWorker(entryWorker.RegisterWorker);
            remoteDel.BeginInvoke(Int32.Parse(args[0]), args[1], null, null);

            System.Console.WriteLine("Press <enter> to terminate worker with ID: " + args[0] + " and URL: " + args[1] + "...");
            System.Console.ReadLine();
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
            IClient client = (IClient)Activator.GetObject(typeof(IClient), url);
            //AsyncCallback asyncCallback = new AsyncCallback(this.CallBack);
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

        //TODO: metodo para o servico status a chamar pelo puppet
        public void StatusRequest() 
        {
            Console.WriteLine("Worker " + getId() + " is alive!");
            //request jobtracker status
            IJobTracker jobTracker = (IJobTracker)Activator.GetObject(typeof(IJobTracker), jobTrackerURL);
            RADRequestJTStatus remoteStat = new RADRequestJTStatus(jobTracker.StatusRequest);
            remoteStat.BeginInvoke(null, null);
        }
    }

    public delegate void DelRegisterWorker(int id, string url);

    class WorkerServices : MarshalByRefObject, IWorker
    {
        Worker worker;
        object mapObject = null;
        Type mapType;

        public WorkerServices(Worker w)
        {
            worker = w;
        }

        public bool SendMapper(String className, byte[] code)
        {
            Console.WriteLine("Received SendMapper");
            Assembly assembly = Assembly.Load(code);
            
            // Walk through each type in the assembly looking for our class
            foreach (Type type in assembly.GetTypes())
            {
                Console.WriteLine("Type: " + type.FullName);
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
            Console.WriteLine("Received SubmitJobToWorker");
            worker.SetClientURL(clientURL);

            IList<KeyValuePair<String, String>> result = new List<KeyValuePair<String, String>>();

            // Client URL must be something like "tcp://localhost:10001/C"
            IClient client = (IClient)Activator.GetObject(typeof(IClient), clientURL);
            byte[] fileSplitByte = client.GetSplit(start, end);
            String filePath = split.ToString() + ".in";
            File.WriteAllBytes(filePath, fileSplitByte);

            // Converts byte[] into a string of the split
            //String fileSplit = Encoding.UTF8.GetString(fileSplitByte);

            // Read each line from the string
            System.IO.StreamReader file = new System.IO.StreamReader(filePath);
            string line;
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
            using (System.IO.StreamWriter outFile = new System.IO.StreamWriter(split.ToString() + ".out"))
            {
                foreach (var l in result)
                {
                    outFile.WriteLine("<" + l.Key + ", " + l.Value + ">");
                }
            }
            Console.WriteLine("Sending result of split: " + split + " to client: " + clientURL);
            worker.SendResultToClient(result, split, clientURL);
            Console.WriteLine("Result sent");
            return worker.getId();
        }

        public void SubmitJobToTracker(long fileSize, int splits, String className, byte[] code, String clientURL)
        {
            Console.WriteLine("Received SubmitJobToTracker");
            worker.SubmitJobToTracker(fileSize, splits, className, code, clientURL);
        }

        public void RegisterWorker(int id, string url)
        {
            Console.WriteLine("Trying to register worker with id: " + id + " and url: " + url);
            worker.RegisterWorker(id, url);
        }

        public void StatusRequest()
        {
            Console.WriteLine("Trying to get status...");
            worker.StatusRequest();
        }
    }
}
