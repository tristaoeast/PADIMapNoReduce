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

        public IList<KeyValuePair<String, String>> GetSplit(long startIndex, long endIndex)
        {
            var stream = new StreamReader(inputFile);
            long start = startIndex, end = endIndex;
            bool exitWhile = false;
            long byteCounter = 0;
            long lineCounter = 0;
            IList<KeyValuePair<String, String>> result = new List<KeyValuePair<String, String>>();
            while (!stream.EndOfStream)
            {
                String line = stream.ReadLine();
                byteCounter += line.Length + 2;
                lineCounter++;
                //Console.WriteLine("bc:" + byteCounter + " lc:" + lineCounter + " line:" + line);
                if (byteCounter >= start + 1)
                {
                    while (byteCounter <= end + 1)
                    {
                        //Console.WriteLine("bc:" + byteCounter + " lc:" + lineCounter + " line:" + line);
                        result.Add(new KeyValuePair<String, String>(lineCounter.ToString(), line));
                        //Read next line
                        if (!stream.EndOfStream)
                        {
                            line = stream.ReadLine();
                            byteCounter += line.Length + 2;
                            lineCounter++;
                        }
                        else
                            byteCounter = end + 10;
                    }
                    exitWhile = true;
                }
                if (exitWhile) break;
            }
            return result;
        }

        //public IList<KeyValuePair<String, String>> GetSplit(long startIndex, long endIndex)
        //{
        //    var stream = new StreamReader(inputFile);
        //    long start = startIndex, end = endIndex;
        //    bool exitWhile = false;
        //    long byteCounter = 0;
        //    long lineCounter = 0;
        //    IList<KeyValuePair<String, String>> result = new List<KeyValuePair<String, String>>();
        //    while (!stream.EndOfStream)
        //    {
        //        String line = stream.ReadLine();
        //        byteCounter += line.Length + 2;
        //        lineCounter++;
        //        //Console.WriteLine("bc:" + byteCounter + " lc:" + lineCounter + " line:" + line);
        //        if (byteCounter >= start + 1)
        //        {
        //            while (byteCounter <= end + 1)
        //            {
        //                //Console.WriteLine("bc:" + byteCounter + " lc:" + lineCounter + " line:" + line);
        //                result.Add(new KeyValuePair<String, String>(lineCounter.ToString(), line));
        //                //Read next line
        //                if (!stream.EndOfStream)
        //                {
        //                    line = stream.ReadLine();
        //                    byteCounter += line.Length + 2;
        //                    lineCounter++;
        //                }
        //                else
        //                    byteCounter = end + 10;
        //            }
        //            exitWhile = true;
        //        }
        //        if (exitWhile) break;
        //    }
        //    return result;
        //}
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

        public IList<KeyValuePair<String, String>> GetSplit(long start, long end)
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
