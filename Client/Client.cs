using MapLib;
using PADIMapNoReduceLibs;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;

namespace Client
{

    public delegate void RADSubmitJob(long fileSize, int splits, string className, byte[] code, string clientURL);

    public class Client
    {
        String inputFile = null;
        String outputFile = null;
        //byte[] fileBytes;
        String entryUrl;
        String clientURL;
        String userAppURL;

        static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine("ERROR: Wrong number of arguments. Expected format: CLIENT <CLIENT-PORT> <USERAPP-URL> <CLIENT-URL>");
                Console.WriteLine("Press any key to exit.");
                Console.ReadLine();
                return;
            }

            Client cli = new Client();
            cli.setClientURL("tcp://" + Dns.GetHostName() + ":" + args[0] + "/C");
            //cli.setClientURL(args[3]);

            TcpChannel chan = new TcpChannel(Int32.Parse(args[0]));
            ChannelServices.RegisterChannel(chan, true);

            //Activation
            ClientServices clientServices = new ClientServices(cli);
            RemotingServices.Marshal(clientServices, "C", typeof(ClientServices));

            Console.WriteLine("Client started. Press any key to exit...");
            Console.ReadLine();
        }

        public void SetEntryURL(String eURL)
        {
            entryUrl = eURL;
        }

        public void SaveDirs(String inputDir, String outputDir)
        {
            inputFile = inputDir;
            outputFile = outputDir;
        }

        public void setClientURL(string url)
        {
            clientURL = url;
        }

        public string getClientURL()
        {
            return clientURL;
        }

        public String GetOutputDir()
        {
            return outputFile;
        }

        public void dbg(String s)
        {
            Console.WriteLine(s);
        }

        public void SubmitJob(long fileSize, int splits, string className, byte[] code)
        {
            Console.WriteLine("Submitting job to tracker at: " + entryUrl + " from client: "+ clientURL);
            IWorker worker = (IWorker)Activator.GetObject(typeof(IWorker), entryUrl);
            //newWorker.SubmitJobToTracker(fileSize, splits, className, code, clientURL);
            RADSubmitJob remoteDel = new RADSubmitJob(worker.SubmitJobToTracker);
            remoteDel.BeginInvoke(fileSize, splits, className, code, clientURL, null, null);

        }

        public byte[] GetSplit(long startIndex, long endIndex)
        {
            var stream = new StreamReader(inputFile);
            long start = startIndex, end = endIndex;
            bool exitWhile = false;
            long byteCounter = 0;
            String result = String.Empty;
            while (!stream.EndOfStream)
            {
                String line = stream.ReadLine();
                byteCounter += line.Length + System.Environment.NewLine.Length;
                //Console.WriteLine("bc:" + byteCounter + " line:" + line);
                if (byteCounter >= start + 1)
                {
                    while (byteCounter <= end + 1)
                    {
                        //Console.WriteLine("bc:" + byteCounter + " line:" + line);
                        result += line + System.Environment.NewLine;
                        //Read next line
                        if (!stream.EndOfStream)
                        {
                            line = stream.ReadLine();
                            byteCounter += line.Length + System.Environment.NewLine.Length;
                        }
                        else
                            byteCounter = end + 10;
                    }
                    exitWhile = true;
                }
                if (exitWhile) break;
            }
            //result = result.Remove(result.Length - System.Environment.NewLine.Length);
            return Encoding.UTF8.GetBytes(result);
        }
    }

    public class ClientServices : MarshalByRefObject, IClient
    {
        public Client client;
        String entryURL = null;
        byte[] code;

        public ClientServices(Client cli)
        {
            client = cli;
        }

        public void Init(String eURL)
        {
            entryURL = eURL;
            client.SetEntryURL(eURL);
        }

        public void Submit(String inputFile, int splits, String outputDirectory, String className, byte[] code)
        {
            //while (String.IsNullOrEmpty(entryURL)) ;

            client.SaveDirs(inputFile, outputDirectory);
            //IWorker newWorker = (IWorker)Activator.GetObject(typeof(IWorker), entryURL);

            //client.setFileBytes();

            FileInfo f = new FileInfo(inputFile);
            long fileSize = f.Length;
            client.SubmitJob(fileSize, splits, className, code);
            //newWorker.SubmitJobToTracker(fileSize, splits, className, code, client.getClientURL());
            client.dbg("New Job submitted to JobTracker at" + entryURL);

        }

        public byte[] GetSplit(long start, long end)
        {
            //byte[] byteArray = client.getFileBytes(start, end);
            //return byteArray;

            return client.GetSplit(start, end);
        }

        public void ReturnResult(IList<KeyValuePair<string, string>> result, int split)
        {
            //guardar no outputFile 
            String outDir = client.GetOutputDir();
            //TODO: make a new thread to write result to file
            Console.WriteLine("Got result from mapping of split: " + split);
            String outFile = outDir + "\\" +  split.ToString() + ".out";
            Console.WriteLine("Writing to file: " + outFile);
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(outFile))
            {
                foreach (var line in result)
                {
                    file.WriteLine("<" + line.Key + ", " + line.Value + ">");
                }
            }
        }
    }
}
