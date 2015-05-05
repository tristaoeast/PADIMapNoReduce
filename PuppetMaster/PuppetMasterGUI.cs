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
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace PuppetMaster
{
    public partial class PuppetMasterGUI : Form
    {
        private String command;
        private String jobTrackerUrl = String.Empty; //serviceurl ou entryurl
        //TODO: botao define port
        private int port = 20001;
        TcpChannel chan;
        PuppetMasterServices appServices;

        public PuppetMasterGUI()
        {
            InitializeComponent();

            //PuppetMasterServices.form = this;

            chan = new TcpChannel(port);
            ChannelServices.RegisterChannel(chan, true);

            //Activation
            appServices = new PuppetMasterServices(this);
            RemotingServices.Marshal(appServices, "PM", typeof(PuppetMasterServices));

            debuuug("PM Services activated");
        }
        public void debuuug(String text)
        {
            tb_Output.AppendText(text + Environment.NewLine);
        }
        private void processCommand(String submText)
        {
            String[] split = submText.Split(null);
            command = split[0];
           // tb_Output.AppendText("Command: "+ command +"\r\n");
            if (command.Equals("Submit", StringComparison.InvariantCultureIgnoreCase))
            {
                if (split.Length == 7)
                {
                    //tb_Output.AppendText(split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
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
                } else
                 //   tb_Output.AppendText(split[1] + " " + split[2] + " " + split[3] + " " + split[split.Length - 1]);
                    Worker(split[1], split[2], split[3], split[split.Length-1]);
            }
            else if (command.Equals("Wait", StringComparison.InvariantCultureIgnoreCase))
            {
                //tb_Output.AppendText("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
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
                //tb_Output.AppendText("Invalid command\r\n");
                return;
            }
        }

        public void Submit(String entryUrl, String inputFile, String outputDir, Int32 splits, String mapClassName, byte[] dll)
        {
            IApp app = (IApp)Activator.GetObject(typeof(IApp), "tcp://localhost:40001/U");
            app.Submit(entryUrl, inputFile, outputDir, splits, mapClassName, dll);
        }

        public void Worker(String id, String puppetMasterUrl, String serviceUrl, String entryUrl)
        {
            debuuug("Start Worker Remote Call... "+ puppetMasterUrl);
            IPuppetMaster puppetW = (IPuppetMaster)Activator.GetObject(typeof(IPuppetMaster), puppetMasterUrl);
            debuuug("1");
            debuuug(puppetW.Worker(id, puppetMasterUrl, serviceUrl, entryUrl));
            debuuug("2");
            jobTrackerUrl = entryUrl;
            debuuug("End Worker Remote Call.. ");
        }

        public void startWorkerProc(String id, String serviceUrl, String jobTrackerUrl)
        {
            debuuug("Start Worker Proc ");
            Process.Start(@"..\..\..\Worker\bin\Debug\Worker.exe", id+" "+serviceUrl+" "+jobTrackerUrl);
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
                    tb_Output.AppendText("Script line being processed: " + line + "\r\n");
                    processCommand(line);
                }
            }
            else
                tb_Output.AppendText("Please enter a command\r\n");
        }

        private void bt_submit_Click(object sender, EventArgs e)
        {
            //Process.Start("C:\\");
            //Process.Start(@"Z:\Documents\Visual Studio 2012\Projects\PADIMapNoReduce\JobTracker\bin\Debug\JobTracker.exe");
            //Process.Start(@"..\..\..\JobTracker\bin\Debug\JobTracker.exe");

            String submittedText;
            if (!string.IsNullOrWhiteSpace(tb_Submit.Text))
            {
                submittedText = tb_Submit.Text;
                processCommand(submittedText);              
            }
            else
                tb_Output.AppendText("Please enter a command\r\n");
        }

    }

    public delegate void DelWorker(String id, String serviceUrl, String entryUrl);
    public delegate void DelDebug(String cenas);

    public class PuppetMasterServices : MarshalByRefObject, IPuppetMaster 
    {
        public static PuppetMasterGUI form;

        public PuppetMasterServices(PuppetMasterGUI f)
        {
            form = f;
        }

        public String Worker(String id, String puppetMasterUrl, String serviceUrl, String entryUrl)
        {
            DelDebug del = new DelDebug(form.debuuug);

            form.Invoke(del, new object[] { "chegueiii 3" });

            if (del == null)
                return "faaaail";
            else return "ola";

            //form.Invoke(del, new Object[] {"chegueiii 3"});
            //form.Invoke(new DelWorker(form.startWorkerProc), new Object[] {id, serviceUrl, entryUrl});
            
        }
    }
}
