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

namespace Worker
{
    class Worker
    {
        static void Main(string[] args)
        {
            TcpChannel channel = new TcpChannel(10000);
            ChannelServices.RegisterChannel(channel, true);
            RemotingConfiguration.RegisterWellKnownServiceType(typeof(IWorkerJT), "Worker", WellKnownObjectMode.Singleton);
            System.Console.WriteLine("Press <enter> to terminate server...");
            System.Console.ReadLine();
        }
    }

    class WorkerServices : IWorkerJT
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
            return true;
        }

        public void SubmitJobToWorker()
        {
            while (mapObject == null) ;

            // Dynamically Invoke the method
            //object[] args = new object[] { "testValue" };
            //object resultObject = mapType.InvokeMember("Map",
            //  BindingFlags.Default | BindingFlags.InvokeMethod,
            //       null,
            //       mapObject,
            //       args);
            //IList<KeyValuePair<string, string>> result = (IList<KeyValuePair<string, string>>)resultObject;
            //Console.WriteLine("Map call result was: ");
            //foreach (KeyValuePair<string, string> p in result)
            //{
            //    Console.WriteLine("key: " + p.Key + ", value: " + p.Value);
            //}
            

        }
    }

}
