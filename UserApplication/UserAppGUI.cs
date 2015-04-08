using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using UserClientLib;

namespace UserApplication
{
    public partial class UserAppGUI : Form
    {
        public UserAppGUI()
        {
            InitializeComponent();
        }

        private void bt_init_Click(object sender, EventArgs e)
        {
            /*ChatClientServices.form = this;
            int port = Int32.Parse(tb_Port.Text);
            TcpChannel chan = new TcpChannel(port);
            ChannelServices.RegisterChannel(chan, false);

            // Alternative 1 for service activation
            ChatClientServices servicos = new ChatClientServices();
            RemotingServices.Marshal(servicos, "ChatClient",
                typeof(ChatClientServices));*/

            // Alternative 2 for service activation
            //RemotingConfiguration.RegisterWellKnownServiceType(
            //    typeof(ChatClientServices), "ChatClient",
            //    WellKnownObjectMode.Singleton);*/

         
            String orig_url = tb_url_cli.Text;

            String url = tb_url_cli.Text;
            url = url.Replace("://", ":");
            //System.Console.WriteLine("{0}", url);

            char[] delimiterChars = {':', '/'};
            string[] split_url = url.Split(delimiterChars);
            //System.Console.WriteLine("{0} words in text:", split_url.Length);

            /*foreach (string s in split_url)
            {
            System.Console.WriteLine(s);
            }*/

            if (Int32.Parse(split_url[2]) < 30001 || Int32.Parse(split_url[2]) > 39999 || split_url[3] != "W" )
            {
                System.Console.WriteLine("Service out of range (Range between 30001 and 39999) or Object name wrong (shoul be 'W')");
            }
            else { 
                 
               /*IClientU client = (IClientU)Activator.GetObject(typeof(IClientU), "tcp://1.2.3.4:" /C");
               IList<KeyValuePair<String, String>> result = client.Submit(..);
               this.server = server;*/
               
            }
        }
    }

    
}
