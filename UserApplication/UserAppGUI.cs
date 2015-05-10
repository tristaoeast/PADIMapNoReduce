using MapLib;
using PADIMapNoReduceLibs;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Net;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace UserApplication
{
    public delegate void RADClientInit(string entryURL);
    public delegate void RADClientSubmit(string inputFile, int splits, string output, string mapName, byte[] code);

    public partial class UserAppGUI : Form
    {
        //IClient client;

        int userPort;
        int clientPort;
        String userAppURL;
        String clientURL;

        bool jobFinished = false;

        UserAppServices appServices;

        public UserAppGUI()
        {
            InitializeComponent();
            //UserAppServices.form = this;

            string[] args = Environment.GetCommandLineArgs();

            if (args.Length < 5)
            {
                tb_UserApp_debug.AppendText("ERROR: Wrong number of arguments. Expected format: USERAPP <USER-PORT [400001-49999]> <USERAPP-URL> <CLIENT-PORT> <CLIENT-URL>");
                tb_UserApp_debug.AppendText("Please close application");
                while (true) ;
            }

            clientURL = "tcp://" + Dns.GetHostName() + ":" + args[3] + "/C";
            userAppURL = args[2];
            userPort = Int32.Parse(args[1]);
            clientPort = Int32.Parse(args[3]);

            TcpChannel chan = new TcpChannel(userPort);
            ChannelServices.RegisterChannel(chan, true);

            //Activation
            appServices = new UserAppServices(this);
            RemotingServices.Marshal(appServices, "U", typeof(UserAppServices));
            tb_UserApp_debug.AppendText("UserApp Service Started on port: " + userPort + Environment.NewLine);
        }

        public void Init(String entryUrl)
        {
            tb_UserApp_debug.AppendText("Starting client with port: " + clientPort + " userURL: " + userAppURL + " clientURL:" + clientURL + Environment.NewLine);
            Process.Start(@"..\..\..\Client\bin\Debug\Client.exe", clientPort + " " + userAppURL + " " + clientURL);
            try
            {
                IClient client = (IClient)Activator.GetObject(typeof(IClient), clientURL);
                //RADClientInit remoteDel = new RADClientInit(client.Init);
                //remoteDel.BeginInvoke(entryUrl, null, null);
                client.Init(entryUrl);
            }
            catch (RemotingException re)
            {
                tb_UserApp_debug.AppendText(re.StackTrace);
            }
            tb_UserApp_debug.AppendText("Client initialized\r\n");
        }

        public void Submit(String inputFile, String outputDirectory, int splits, String mapClassName, byte[] code, String entryUrl)
        {
            IClient client = (IClient)Activator.GetObject(typeof(IClient), clientURL);
            //tb_UserApp_debug.AppendText(Environment.CurrentDirectory + Environment.NewLine);
            //tb_UserApp_debug.AppendText("inFile: " + inputFile + Environment.NewLine + " splits: " + splits + Environment.NewLine + " outDir: " + outputDirectory + Environment.NewLine + " mapclassName: " + mapClassName + Environment.NewLine);
            tb_UserApp_debug.AppendText("Job from input file: " + inputFile + " and splits: " + splits + " submitted." + Environment.NewLine);
            client.Submit(inputFile, splits, outputDirectory, mapClassName, code);
            while (!appServices.isJobFinished()) ;

            tb_UserApp_debug.AppendText("Job finished\r\n");
        }

        public void setJobFinished(bool finisehd)
        {
            jobFinished = finisehd;
        }

        //public void createUserApp(int clientPort, String userURL, String cliURL)
        //{
        //    clientURL = cliURL;
        //    userAppURL = userURL;
        //    port = clientPort;

        //    TcpChannel chan = new TcpChannel(port);
        //    ChannelServices.RegisterChannel(chan, false);

        //    //Activation
        //    UserAppServices appServices = new UserAppServices(this);
        //    RemotingServices.Marshal(appServices, "U", typeof(UserAppServices));
        //}

        private void bt_init_Click(object sender, EventArgs e)
        {
            String orig_url = tb_url_cli.Text;

            String url = tb_url_cli.Text;
            url = url.Replace("://", ":");

            char[] delimiterChars = { ':', '/' };
            string[] split_url = url.Split(delimiterChars);

            if (Int32.Parse(split_url[2]) < 30001 || Int32.Parse(split_url[2]) > 39999 || split_url[3] != "W")
            {
                tb_UserApp_debug.AppendText("Service out of range (Range between 30001 and 39999) or Object name wrong (shoul be 'W')\r\n");
            }
            else
            {
                Init(url);
            }
        }
    }

    delegate void DelInit(String entryUrl);
    delegate void DelSubmit(String inputFile, String outputDirectory, int splits, String mapClassName, byte[] code, String entryURL);
    delegate void DelJobFinished(bool finished);

    public class UserAppServices : MarshalByRefObject, IApp
    {
        public static UserAppGUI form;
        bool jobFinished = false;

        public UserAppServices(UserAppGUI f)
        {
            form = f;
        }

        public bool isJobFinished()
        {
            return jobFinished;
        }


        public void Submit(String entryUrl, String inputFile, String outputDirectory, int splits, String mapClassName, byte[] code)
        {
            form.Invoke(new DelInit(form.Init), entryUrl);
            form.Invoke(new DelSubmit(form.Submit), new Object[] { inputFile, outputDirectory, splits, mapClassName, code, entryUrl });
        }

        public void notifyJobFinished(bool finished)
        {
            jobFinished = finished;
        }
    }

}
