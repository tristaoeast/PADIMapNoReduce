using MapLib;
using PADIMapNoReduceLibs;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.IO;
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
using System.Windows.Forms;

namespace PuppetMaster
{

    public delegate String RemoteAsyncDelegateNewWorker(String id, String serviceUrl, String entryUrl);
    public delegate void FormWriteToOutput(String text);
    public delegate void RADRequestWStatus();

    public partial class PuppetMasterGUI : Form
    {
        private String command;
        private String jobTrackerUrl = String.Empty; //serviceurl ou entryurl
        //default value for the PM port
        private int port = 20001;
        TcpChannel chan;
        PuppetMasterServices appServices;
        int clientPortCounter = 10000;
        int userPortCounter = 40000;
        List<String> workersList = new List<string>();

        public PuppetMasterGUI()
        {
            InitializeComponent();

            //PuppetMasterServices.form = this;
            PuppetMasterServices.form = this;
            chan = new TcpChannel(port);
            ChannelServices.RegisterChannel(chan, true);


            //Activation
            appServices = new PuppetMasterServices();
            RemotingServices.Marshal(appServices, "PM", typeof(PuppetMasterServices));
            dbg(Dns.GetHostName());

            dbg("PM Services activated");
        }
        public void dbg(String text)
        {
            tb_Output.AppendText(text + Environment.NewLine);
        }

        private void processCommand(String submText)
        {
            String[] split = submText.Split(null);
            command = split[0];
            if (command.Equals("%", StringComparison.InvariantCultureIgnoreCase))
                this.Invoke(new FormWriteToOutput(this.dbg), new object[] { "Ignoring line..." });
            //dbg("Ignoring line..." + Environment.NewLine);
            else if (command.Equals("Submit", StringComparison.InvariantCultureIgnoreCase))
            {
                if (split.Length == 7)
                {
                    Submit(split[1], split[2], split[3], Int32.Parse(split[4]), split[5], File.ReadAllBytes(split[6]));
                }

                else
                {
                    this.Invoke(new FormWriteToOutput(this.dbg), new object[] { "ERROR: Submit command must have 6 arguments" });
                    //dbg("Wrong number of args. Submit command must have 6 arguments");
                }
            }
            else if (command.Equals("Worker", StringComparison.InvariantCultureIgnoreCase))
            {
                if (split.Length < 4)
                {
                    this.Invoke(new FormWriteToOutput(this.dbg), new object[] { "ERROR: WORKER command must have at least 3 arguments" });
                    //dbg("Wrong number of args. Worker command must have at least 3 arguments");
                }
                else
                {
                    //   dbg(split[1] + " " + split[2] + " " + split[3] + " " + split[split.Length - 1]);
                    Worker(split[1], split[2], split[3], split[split.Length - 1]);
                }
            }
            else if (command.Equals("Wait", StringComparison.InvariantCultureIgnoreCase))
            {
                //dbg("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
                this.Invoke(new FormWriteToOutput(this.dbg), new object[] { "Waiting " + split[1] + " seconds." });
                Wait(Int32.Parse(split[1]));
            }
            else if (command.Equals("Status", StringComparison.InvariantCultureIgnoreCase))
            {
                if (split.Length != 1)
                    this.Invoke(new FormWriteToOutput(this.dbg), new object[] { "ERROR: STATUS command has no arguments" });
                else
                    Status();   
            }
            else if (command.Equals("SlowW", StringComparison.InvariantCultureIgnoreCase))
            {
                //dbg("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);0
            }
            else if (command.Equals("FreezeW", StringComparison.InvariantCultureIgnoreCase))
            {
                //dbg("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
            }
            else if (command.Equals("UnfreezeW", StringComparison.InvariantCultureIgnoreCase))
            {
                //dbg("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
            }
            else if (command.Equals("FreezeC", StringComparison.InvariantCultureIgnoreCase))
            {
                //dbg("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
            }
            else if (command.Equals("UnfreezeC", StringComparison.InvariantCultureIgnoreCase))
            {
                //dbg("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
            }
            else
            {
                this.Invoke(new FormWriteToOutput(this.dbg), new object[] { "ERROR: Invalid command" });
                //dbg("Invalid command" + Environment.NewLine);
                return;
            }
        }

        public void Submit(String entryUrl, String inputFile, String outputDir, int splits, String mapClassName, byte[] dll)
        {
            clientPortCounter++;
            userPortCounter++;
            //dbg(clientPortCounter.ToString());
            Process.Start(@"..\..\..\UserApplication\bin\Debug\UserApplication.exe", userPortCounter + " " + "tcp://localhost:" + userPortCounter.ToString() + "/U" + " " + clientPortCounter + " " + "tcp://localhost:" + clientPortCounter.ToString() + "/C");

            //for (int i = 0; i < 1000000000; i++)
            //{
            //    int j = 10000 / 3000;
            //}

            //IApp app = (IApp)Activator.GetObject(typeof(IApp), "tcp://localhost:" + clientPortCounter.ToString() + "/U");
            IApp app = (IApp)Activator.GetObject(typeof(IApp), "tcp://localhost:" + userPortCounter.ToString() + "/U");
            try
            {
                app.Submit(entryUrl, inputFile, outputDir, splits, mapClassName, dll);
            }
            catch (RemotingException re)
            {
                this.Invoke(new FormWriteToOutput(this.dbg), new object[] { "RemotingException: " + re.StackTrace });
                //dbg("Remoting Exception: " + re.StackTrace + Environment.NewLine);
            }
            catch (SocketException e)
            {
                this.Invoke(new FormWriteToOutput(this.dbg), new object[] { "SocketException: " + e.ErrorCode });
                //dbg("Socket Exception: " + e.ErrorCode.ToString() + Environment.NewLine);
            }
        }

        public void Worker(String id, String puppetMasterUrl, String serviceUrl, String entryUrl)
        {
            this.Invoke(new FormWriteToOutput(this.dbg), new object[] { "Start Worker remote call... " + puppetMasterUrl });
            //dbg("Start Worker Remote Call... " + puppetMasterUrl);
            IPuppetMaster puppetW = (IPuppetMaster)Activator.GetObject(typeof(IPuppetMaster), puppetMasterUrl);

            //AsyncCallback asyncCallback = new AsyncCallback(this.CallBack);
            RemoteAsyncDelegateNewWorker remoteDel = new RemoteAsyncDelegateNewWorker(puppetW.Worker);
            remoteDel.BeginInvoke(id, serviceUrl, entryUrl, null, null);
            //remoteDel.BeginInvoke(id, serviceUrl, entryUrl, asyncCallback, null);

            jobTrackerUrl = entryUrl;
            this.Invoke(new FormWriteToOutput(this.dbg), new object[] { "End Worker remote call..." + puppetMasterUrl });
            //dbg("End Worker Remote Call.. ");
        }

        //We aren't using this since we don't want to return anything. But it works as a reminder/example of how to
        public void CallBack(IAsyncResult ar)
        {
            RemoteAsyncDelegateNewWorker rad = (RemoteAsyncDelegateNewWorker)((AsyncResult)ar).AsyncDelegate;
            String s = (String)rad.EndInvoke(ar);
            this.Invoke(new FormWriteToOutput(this.dbg), new object[] { s });
            //dbg(s); DOESNT WORK!!! INVOKE NEEDED LIKE ABOVE

        }

        public void startWorkerProc(String id, String serviceUrl, String jobTrackerUrl)
        {
            this.Invoke(new FormWriteToOutput(this.dbg), new object[] { "Start Worker Process with id " + id});

            //dbg("Start Worker Proc ");
            workersList.Add(serviceUrl);
            Process.Start(@"..\..\..\Worker\bin\Debug\Worker.exe", id + " " + serviceUrl + " " + jobTrackerUrl);
        }

        public void Wait(int secs)
        {
            int interval = secs * 1000;
            Thread.Sleep(interval);
        }

        public void Status()
        { 
            foreach (String wURL in workersList){
                //this.Invoke(new FormWriteToOutput(this.dbg), new object[] {"workersLists: " + wURL + " STATUS request" });
                IWorker worker = (IWorker)Activator.GetObject(typeof(IWorker), wURL);
                RADRequestWStatus remoteStat = new RADRequestWStatus(worker.StatusRequest);
                remoteStat.BeginInvoke(null, null);
            }
        }

        public void loadScript(string scriptPath)
        {
            String line;
            System.IO.StreamReader file;
            try
            {
                file = new System.IO.StreamReader(scriptPath);
            }
            catch (FileNotFoundException)
            {
                this.Invoke(new FormWriteToOutput(this.dbg), new object[] { "ERROR: The following script was not found: " + scriptPath });
                return;
            }
            while ((line = file.ReadLine()) != null)
            {
                processCommand(line);
            }
        }

        private void bt_loadScript_Click(object sender, EventArgs e)
        {
            String pathToScript;

            if (!string.IsNullOrWhiteSpace(tb_loadScript.Text))
            {
                pathToScript = tb_loadScript.Text;

                Thread loaderThread = new Thread(() => loadScript(pathToScript));
                loaderThread.Start();

            }
            else
                dbg("Please enter a command" + Environment.NewLine);
        }

        private void bt_submit_Click(object sender, EventArgs e)
        {
            //String submittedText = "WORKER 1 tcp://localhost:20001/PM tcp://localhost:30001/W";
            String submittedText;

            if (!string.IsNullOrWhiteSpace(tb_Submit.Text))
            {
                submittedText = tb_Submit.Text;
                processCommand(submittedText);
            }
            else
                dbg("Please enter a command" + Environment.NewLine);
        }

    }

    public delegate void DelWorker(String id, String serviceUrl, String entryUrl);
    public delegate void DelDebug(string cenas);
    public delegate void DelStatus();

    public class PuppetMasterServices : MarshalByRefObject, IPuppetMaster
    {
        public static PuppetMasterGUI form;

        public String Worker(String id, String serviceUrl, String entryUrl)
        {
            DelDebug del = new DelDebug(form.dbg);

            form.Invoke(del, new object[] { "Received ID: " + id + " ServiceURL: " + serviceUrl + " EntryURL: " + entryUrl });
            form.Invoke(new DelWorker(form.startWorkerProc), new Object[] { id, serviceUrl, entryUrl });

            return "Sucessfully launched a new Worker";
        }

        public void Status(int id, String url)
        {
            form.Invoke(new DelStatus(form.Status), null);
        }
    }
}
