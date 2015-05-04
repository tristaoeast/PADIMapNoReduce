using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using JobTrackerWorkerLib;
using JobTrackerClientLib;
using System.Reflection;
using WorkerClientLib;
using System.IO;

namespace Worker
{
    class Worker
    {
        static void Main(string[] args)
        {
            //TODO: get port from args
            TcpChannel channel = new TcpChannel(10000);
            ChannelServices.RegisterChannel(channel, true);

            //Activation
            WorkerServicesToJobTracker servicosToJobTracker = new WorkerServicesToJobTracker();
            RemotingServices.Marshal(servicosToJobTracker, "W", typeof(WorkerServicesToJobTracker));

            //Activation
            WorkerServicesToClient servicosToClient = new WorkerServicesToClient();
            RemotingServices.Marshal(servicosToClient, "W", typeof(WorkerServicesToClient));

            //RemotingConfiguration.RegisterWellKnownServiceType(typeof(IWorkerJT), "W", WellKnownObjectMode.Singleton);
            System.Console.WriteLine("Press <enter> to terminate server...");
            System.Console.ReadLine();
        }
    }

    class WorkerServicesToJobTracker : MarshalByRefObject, IWorkerJT
    {

        object mapObject = null;
        Type mapType;

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

        public void SubmitJobToWorker(long start, long end, int split)
        {
            IClientW client = (IClientW)Activator.GetObject(typeof(IClientW), "tcp://localhost:10001/C");
            byte[] fileSplitByte = client.GetSplit(start, end);
            while (mapObject == null) ;
            
            // Converts byte[] into a string of the split
            String fileSplit = Encoding.UTF8.GetString(fileSplitByte);

            // Read each line from the string
            using (StringReader reader = new StringReader(fileSplit))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    // Dynamically Invoke the method
                    object[] args = new object[] {line};
                    object resultObject = mapType.InvokeMember("Map",
                      BindingFlags.Default | BindingFlags.InvokeMethod,
                           null,
                           mapObject,
                           args);
                    IList<KeyValuePair<string, string>> result = (IList<KeyValuePair<string, string>>)resultObject;
                    Console.WriteLine("Map call result was: ");
                    foreach (KeyValuePair<string, string> p in result)
                    {
                        Console.WriteLine("key: " + p.Key + ", value: " + p.Value);
                    }
                }
            }
        }
    }

    class WorkerServicesToClient : MarshalByRefObject, IWorkerC
    {
        public void SubmitJob(long fileSize, int splits, String className, byte[] code) 
        {
            IJobTrackerW newJobTracker = (IJobTrackerW)Activator.GetObject(typeof(IJobTrackerW), "METER_URL_BEM");
            newJobTracker.submitJob(fileSize, splits, className, code);
        }
    }

}
