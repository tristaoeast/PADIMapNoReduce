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

    public partial class PuppetMasterGUI : Form
    {
        private String command;
        private String jobTrackerUrl = String.Empty; //serviceurl ou entryurl
        //default value for the PM port
        private int port = 20001;
        TcpChannel chan;
        PuppetMasterServices appServices;
        int clientPortCounter = 10000;

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
                tb_Output.AppendText("Ignoring line..." + Environment.NewLine);
            else if (command.Equals("Submit", StringComparison.InvariantCultureIgnoreCase))
            {
                tb_Output.AppendText(Environment.CurrentDirectory + Environment.NewLine);
                if (split.Length == 7)
                {
                    tb_Output.AppendText("Command length: " + split.Length + Environment.NewLine);
                    Submit(split[1], split[2], split[3], Int32.Parse(split[4]), split[5], File.ReadAllBytes(split[6]));
                }

                else
                    tb_Output.AppendText("Wrong number of args. Submit command must have 6 arguments");
            }
            else if (command.Equals("Worker", StringComparison.InvariantCultureIgnoreCase))
            {
                if (split.Length < 4)
                {
                    tb_Output.AppendText("Wrong number of args. Worker command must have at least 3 arguments");
                }
                else
                {
                    //   tb_Output.AppendText(split[1] + " " + split[2] + " " + split[3] + " " + split[split.Length - 1]);
                    Worker(split[1], split[2], split[3], split[split.Length - 1]);
                }
            }
            else if (command.Equals("Wait", StringComparison.InvariantCultureIgnoreCase))
            {
                //tb_Output.AppendText("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
                //Wait(Int32.Parse(split[1]);
            }
            else if (command.Equals("Status", StringComparison.InvariantCultureIgnoreCase))
            {
                //tb_Output.AppendText("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
            }
            else if (command.Equals("SlowW", StringComparison.InvariantCultureIgnoreCase))
            {
                //tb_Output.AppendText("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);0
            }
            else if (command.Equals("FreezeW", StringComparison.InvariantCultureIgnoreCase))
            {
                //tb_Output.AppendText("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
            }
            else if (command.Equals("UnfreezeW", StringComparison.InvariantCultureIgnoreCase))
            {
                //tb_Output.AppendText("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
            }
            else if (command.Equals("FreezeC", StringComparison.InvariantCultureIgnoreCase))
            {
                //tb_Output.AppendText("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
            }
            else if (command.Equals("UnfreezeC", StringComparison.InvariantCultureIgnoreCase))
            {
                //tb_Output.AppendText("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
            }
            else
            {
                tb_Output.AppendText("Invalid command" + Environment.NewLine);
                return;
            }
        }

        public void Submit(String entryUrl, String inputFile, String outputDir, Int32 splits, String mapClassName, byte[] dll)
        {
            clientPortCounter++;
            tb_Output.AppendText(clientPortCounter.ToString());
            Process.Start(@"..\..\..\UserApplication\bin\Debug\UserApplication.exe", clientPortCounter + " " + "tcp://localhost:" + clientPortCounter.ToString() + "/U" + " " + "tcp://localhost:" + clientPortCounter.ToString() + "/C");


            for (int i = 0; i < 1000000000; i++)
            {
                int j = 10000 / 3000;
            }
            //TODO: arrancar processo da app antes desta cangalhada toda


            IApp app = (IApp)Activator.GetObject(typeof(IApp), "tcp://localhost:" + clientPortCounter.ToString() + "/U");
            try
            {
                tb_Output.AppendText(clientPortCounter.ToString());
                app.Submit(entryUrl, inputFile, outputDir, splits, mapClassName, dll);
            }
            catch (RemotingException re)
            {
                tb_Output.AppendText("Remoting Exception: " + re.StackTrace + Environment.NewLine);
            }
            catch (SocketException e)
            {
                tb_Output.AppendText("Exceeeeeption: " + e.ErrorCode.ToString() + Environment.NewLine);
            }
        }

        public void Worker(String id, String puppetMasterUrl, String serviceUrl, String entryUrl)
        {
            dbg("Start Worker Remote Call... " + puppetMasterUrl);
            IPuppetMaster puppetW = (IPuppetMaster)Activator.GetObject(typeof(IPuppetMaster), puppetMasterUrl);

            //AsyncCallback asyncCallback = new AsyncCallback(this.CallBack);
            RemoteAsyncDelegateNewWorker remoteDel = new RemoteAsyncDelegateNewWorker(puppetW.Worker);
            remoteDel.BeginInvoke(id, serviceUrl, entryUrl, null, null);
            //remoteDel.BeginInvoke(id, serviceUrl, entryUrl, asyncCallback, null);

            jobTrackerUrl = entryUrl;
            dbg("End Worker Remote Call.. ");
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
            dbg("Start Worker Proc ");
            Process.Start(@"..\..\..\Worker\bin\Debug\Worker.exe", id + " " + serviceUrl + " " + jobTrackerUrl);
        }

        public void Wait(int secs)
        {
            int interval = secs * 1000; //the call is in milliseconds
            //TODO: put the command processing stuff to sleep for x secs
            //someThread.sleep(secs);
        }

        private void bt_loadScript_Click(object sender, EventArgs e)
        {
            String pathToScript;

            if (!string.IsNullOrWhiteSpace(tb_loadScript.Text))
            {
                pathToScript = tb_loadScript.Text;
                String line;

                System.IO.StreamReader file = new System.IO.StreamReader(pathToScript);
                while ((line = file.ReadLine()) != null)
                {
                    tb_Output.AppendText("Script line being processed: " + line + Environment.NewLine);
                    processCommand(line);
                }
            }
            else
                tb_Output.AppendText("Please enter a command" + Environment.NewLine);
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
                tb_Output.AppendText("Please enter a command" + Environment.NewLine);
        }

        private void bt_pmPort_Click(object sender, EventArgs e)
        {
            int req_port;
            if (!string.IsNullOrWhiteSpace(tb_pmPort.Text))
            {
                req_port = Int32.Parse(tb_pmPort.Text);
                if (req_port < 30000 && req_port > 20000)
                {
                    dbg("Port before click: " + port);
                    port = req_port;
                    dbg("Port after click: " + port);
                }
                else
                    tb_Output.AppendText("Requested Port out of range! Must be between 20001 and 29999" + Environment.NewLine);
            }
            else
                tb_Output.AppendText("Default PuppetMaster port 20001 being used" + Environment.NewLine);
        }



    }

    public delegate void DelWorker(String id, String serviceUrl, String entryUrl);
    public delegate void DelDebug(string cenas);

    public class PuppetMasterServices : MarshalByRefObject, IPuppetMaster
    {
        public static PuppetMasterGUI form;

        public String Worker(String id, String serviceUrl, String entryUrl)
        {
            DelDebug del = new DelDebug(form.dbg);

            //form.Invoke(del, new object[] { "Received ID: " + id + " ServiceURL: " + serviceUrl + " EntryURL: " + entryUrl });
            form.Invoke(new DelWorker(form.startWorkerProc), new Object[] { id, serviceUrl, entryUrl });

            return "Sucessfully launched a new Worker";
        }
    }
}
