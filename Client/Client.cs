using MapLib;
using PADIMapNoReduceLibs;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;

namespace Client
{
    public class Client
    {
        String inputFile = null;
        String outputFile = null;
        //byte[] fileBytes;
        String entryUrl;
        String clientURL;

        static void Main(string[] args)
        {
            //TcpChannel chan = new TcpChannel(10001);
            //ChannelServices.RegisterChannel(chan, true);

            Client cli = new Client();
            //Activation
            ClientServices clientServices = new ClientServices(cli);
            RemotingServices.Marshal(clientServices, "C", typeof(ClientServices));

            if (args.Length < 3)
            {
                Console.WriteLine("ERROR: Wrong number of arguments. Expected format: CLIENT <PORT> <USERAPP-URL> <CLIENT-URL>");
                Console.WriteLine("Press any key to exit.");
                Console.ReadLine();
                return;
            }

            cli.setClientURL(args[2]);
            
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

        //public void setFileBytes()
        //{
        //    fileBytes = File.ReadAllBytes(inputFile);
        //}

        //public byte[] getFileBytes(long init, long end)
        //{
        //    byte[] subset = new byte[end - init + 1];
        //    Array.Copy(fileBytes, init, subset, 0, end - init + 1);

        //    return subset;
        //}

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
        String urlJobTracker = null;
        byte[] code;

        public ClientServices(Client cli)
        {
            client = cli;
        }

        public void Init(String entryURL)
        {
            client.dbg("PASSEI NO INIT");
            urlJobTracker = entryURL;
            client.SetEntryURL(entryURL);
        }
        public void Submit(String inputFile, int splits, String outputDirectory, String className, byte[] code)
        {
            //while (String.IsNullOrEmpty(urlJobTracker)) ;

            client.dbg("New Job submitted to JobTracker at" + urlJobTracker);

            client.SaveDirs(inputFile, outputDirectory);
            IWorker newWorker = (IWorker)Activator.GetObject(typeof(IWorker), urlJobTracker);

            //client.setFileBytes();

            FileInfo f = new FileInfo(inputFile);
            long fileSize = f.Length;

            newWorker.SubmitJobToTracker(fileSize, splits, className, code, client.getClientURL());

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
