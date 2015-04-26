using MapLib;
using PuppetAppLib;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using UserClientLib;

namespace UserApplication
{
    public partial class UserAppGUI : Form
    {
        IClientU client;

        public UserAppGUI()
        {
            InitializeComponent();
            UserAppServices.form = this;

            TcpChannel chan = new TcpChannel(40001);
            ChannelServices.RegisterChannel(chan, false);

            //Activation
            UserAppServices servicosToPuppet = new UserAppServices();
            RemotingServices.Marshal(servicosToPuppet, "U", typeof(UserAppServices));

        }

        public void Init(String entryUrl){
            client = (IClientU)Activator.GetObject(typeof(IClientU), "tcp://localhost:10001/C");
            client.Init(entryUrl);
            tb_UserApp_debug.AppendText("Client initialized\r\n");
        }

        public void Submit(String inputFile, String outputDirectory, Int32 splits, String mapClassName, IMap mapObject){
            client.Submit(inputFile, splits, outputDirectory, mapObject);
            tb_UserApp_debug.AppendText("Job submitted to client\r\n");
        } 
 
        private void bt_init_Click(object sender, EventArgs e)
        {
            String orig_url = tb_url_cli.Text;

            String url = tb_url_cli.Text;
            url = url.Replace("://", ":");

            char[] delimiterChars = {':', '/'};
            string[] split_url = url.Split(delimiterChars);

            if (Int32.Parse(split_url[2]) < 30001 || Int32.Parse(split_url[2]) > 39999 || split_url[3] != "W" )
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
    delegate void DelSubmit(String inputFile, String outputDirectory, Int32 splists, String mapClassName, IMap mapObject);

    public class UserAppServices : MarshalByRefObject, IAppPuppet
    {
        public static UserAppGUI form;

        public void Submit(String entryUrl, String inputFile, String outputDirectory, Int32 splits, String mapClassName, IMap mapObject)
        {
            form.Invoke(new DelInit(form.Init), entryUrl);
            form.Invoke(new DelSubmit(form.Submit), new Object[] {inputFile, outputDirectory, splits, mapClassName, mapObject });
        }
    }
    
}
