﻿using MapLib;
using PADIMapNoReduceLibs;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace UserApplication
{
    public partial class UserAppGUI : Form
    {
        IClient client;

        int port;
        String userAppURL;
        String clientURL;

        public UserAppGUI()
        {
            InitializeComponent();
            UserAppServices.form = this;

            string[] args = Environment.GetCommandLineArgs();

            if (args.Length < 3)
            {
               Console.WriteLine("ERROR: Wrong number of arguments. Expected format: USERAPP <PORT> <USERAPP-URL> <CLIENT-URL>");
                Console.WriteLine("Press any key to exit.");
                Console.ReadLine();
                return;
            }

            clientURL = args[2];
            userAppURL = args[1];
            port = Int32.Parse(args[0]);



            TcpChannel chan = new TcpChannel(port);
            ChannelServices.RegisterChannel(chan, false);

            //Activation
            UserAppServices appServices = new UserAppServices();
            RemotingServices.Marshal(appServices, "U", typeof(UserAppServices));

        }

        public void Init(String entryUrl)
        {
            Process.Start(@"..\..\..\Client\bin\Debug\Client.exe", port + " " + userAppURL + " " + clientURL);
            client = (IClient)Activator.GetObject(typeof(IClient), "tcp://localhost:10001/C");
            client.Init(entryUrl);
            tb_UserApp_debug.AppendText("Client initialized\r\n");
        }

        public void Submit(String inputFile, String outputDirectory, Int32 splits, String mapClassName, byte[] code)
        {
            client.Submit(inputFile, splits, outputDirectory, mapClassName, code);
            tb_UserApp_debug.AppendText("Job submitted to client\r\n");
        }

        public void createUserApp(int clientPort, String userURL, String cliURL)
        {
            clientURL = cliURL;
            userAppURL = userURL;
            port = clientPort;

            TcpChannel chan = new TcpChannel(port);
            ChannelServices.RegisterChannel(chan, false);

            //Activation
            UserAppServices appServices = new UserAppServices();
            RemotingServices.Marshal(appServices, "U", typeof(UserAppServices));
        }

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
    delegate void DelSubmit(String inputFile, String outputDirectory, Int32 splists, String mapClassName, byte[] code);

    public class UserAppServices : MarshalByRefObject, IApp
    {
        public static UserAppGUI form;

        public void Submit(String entryUrl, String inputFile, String outputDirectory, Int32 splits, String mapClassName, byte[] code)
        {
            form.Invoke(new DelInit(form.Init), entryUrl);
            form.Invoke(new DelSubmit(form.Submit), new Object[] { inputFile, outputDirectory, splits, mapClassName, code});
        }
    }

}
