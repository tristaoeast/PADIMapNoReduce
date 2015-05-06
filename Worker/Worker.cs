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

namespace Worker
{

    public delegate void RemoteAsyncDelegateSendResultsToClient(IList<KeyValuePair<string,string>> result, int split);

    class Worker
    {
        String jobTrackerURL = String.Empty;
        String clientURL = String.Empty;
        int myId;

        static void Main(string[] args)
        {
            Worker w = new Worker();

            if (args.Length < 2)
            {
                Console.WriteLine("Wrong number of arguments. Expected format: WORKER <ID> <SERVICE-URL> <JOBTRACKER-URL> ");
                Console.WriteLine("Press any key to exit.");
                Console.ReadLine();
                return;
            }
            else if (3 == args.Length)
            {
                //WARN JT THAT STARTED
                w.SetJobTrackerURL(args[2]);
            }

            w.setId(Int32.Parse(args[0]));

            //TODO: get port from service url
            string[] split1 = args[1].Split(':');
            string[] split2 = split1[2].Split('/');
            int port = Int32.Parse(split2[0]);
            TcpChannel channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(channel, true);

            //Activation
            WorkerServices workerServices = new WorkerServices(w);
            RemotingServices.Marshal(workerServices, "W", typeof(WorkerServices));

            //RemotingConfiguration.RegisterWellKnownServiceType(typeof(IWorkerJT), "W", WellKnownObjectMode.Singleton);
            System.Console.WriteLine("Press <enter> to terminate worker with ID: " + args[0] + "...");
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

        public void sendResultToClient(IList<KeyValuePair<string,string>> result, int split, string url) {
            IClient client = (IClient)Activator.GetObject(typeof(IClient), url);
            //AsyncCallback asyncCallback = new AsyncCallback(this.CallBack);
            RemoteAsyncDelegateSendResultsToClient remoteDel = new RemoteAsyncDelegateSendResultsToClient(client.ReturnResult);
            remoteDel.BeginInvoke(result, split, null, null);
        }
    }

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
            Assembly assembly = Assembly.Load(code);
            // Walk through each type in the assembly looking for our class
            foreach (Type type in assembly.GetTypes())
            {
                if (type.IsClass == true)
                {
                    if (type.FullName.EndsWith("." + className))
                    {
                        mapType = type;
                        // create an instance of the object
                        mapObject = Activator.CreateInstance(type);
                        return true;

                    }
                }
            }
            throw (new System.Exception("could not invoke method"));
        }

        public int SubmitJobToWorker(long start, long end, int split, string clientURL)
        {
            worker.SetClientURL(clientURL);

            IList<KeyValuePair<String, String>> result = new List<KeyValuePair<String,String>>();

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
                Console.WriteLine("Map call result for line: " + line + "  was: ");
                foreach (KeyValuePair<string, string> p in result)
                {
                    Console.WriteLine("key: " + p.Key + ", value: " + p.Value);
                }
            }
            worker.sendResultToClient(result, split, clientURL);
            return worker.getId();
        }

        public void SubmitJobToTracker(long fileSize, int splits, String className, byte[] code, String clientURL)
        {
            IJobTracker newJobTracker = (IJobTracker)Activator.GetObject(typeof(IJobTracker), worker.GetJobTrackerURL());
            newJobTracker.SubmitJob(fileSize, splits, className, code, clientURL);
        }
    }
}
