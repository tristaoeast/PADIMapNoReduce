using JobTrackerClientLib;
using MapLib;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;
using UserClientLib;
using WorkerClientLib;

namespace Client
{
    class Client
    {
        String inputFile = null;
        String outputFile = null;
        byte[] fileBytes;

        static void Main(string[] args)
        {
            TcpChannel chan = new TcpChannel(10001);
            ChannelServices.RegisterChannel(chan, false);

            Client cli = new Client();
            //Activation
            ClientServicesToApp servicosToApp = new ClientServicesToApp(cli);
            RemotingServices.Marshal(servicosToApp, "C", typeof(ClientServicesToApp));

            //Activation
            ClientServicesToWorker servicosToW = new ClientServicesToWorker(cli);
            RemotingServices.Marshal(servicosToW, "C", typeof(ClientServicesToWorker));
        }


        public void SaveDirs(String inputDir, String outputDir)
        {
            inputFile = inputDir;
            outputFile = outputDir;
        }

        public String GetOutputDir()
        {
            return outputFile;
        }

        public void setFileBytes()
        {
            fileBytes = File.ReadAllBytes(inputFile);
        }

        public byte[] getFileBytes(long init, long end)
        {
            byte[] subset = new byte[end - init + 1];
            Array.Copy(fileBytes, init, subset, 0, end - init + 1);

            return subset;
        }
    }

    public class ClientServicesToApp : MarshalByRefObject, IClientU
    {
        public Client client;
        String urlJobTracker = null;
        byte[] code;

        public ClientServicesToApp(Client cli)
        {
            client = cli;
        }

        public void Init(String entryURL)
        {
            urlJobTracker = entryURL;
        }
        public void Submit(String inputFile, int splits, String outputDirectory, IMap mapObject)
        {
            while (String.IsNullOrEmpty(urlJobTracker)) ;

            Console.WriteLine("New Job submitted to JobTracker at" + urlJobTracker);

            client.SaveDirs(inputFile, outputDirectory);
            IJobTrackerC newJobTracker = (IJobTrackerC)Activator.GetObject(typeof(IJobTrackerC), urlJobTracker);

            client.setFileBytes();

            FileInfo f = new FileInfo(inputFile);
            long fileSize = f.Length;

            newJobTracker.submitJob(fileSize, splits);
        }
    }

    public class ClientServicesToWorker : MarshalByRefObject, IClientW
    {
        public static Client client;

        public ClientServicesToWorker(Client cli)
        {
            client = cli;
        }

        public byte[] GetSplit(long start, long end)
        {
            byte[] byteArray = client.getFileBytes(start, end);
            return byteArray;
        }

        public void ReturnResult(IList<KeyValuePair<string, string>> result, int split)
        {
            //guardar no outputFile 
            String outDir = client.GetOutputDir();
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(outDir + split.ToString() + ".out"))
            {
                foreach (var line in result)
                {
                    file.WriteLine("<" + line.Key + ", " + line.Value + ">");
                }
            }
        }
    }
}
