using MapLib;
using PuppetAppLib;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace PuppetMaster
{
    public partial class PuppetMasterGUI : Form
    {
        private String command;
        private String[] args;

        public PuppetMasterGUI()
        {
            InitializeComponent();
        }

        private void Submit(String entryUrl, String inputFile, String outputDir, Int32 splits, String mapClassName, byte[] dll)
        {
            IAppPuppet app = (IAppPuppet)Activator.GetObject(typeof(IAppPuppet), "tcp://localhost:40001/U");
            app.Submit(entryUrl, inputFile, outputDir, splits, mapClassName, dll);
        }

        private void bt_submit_Click(object sender, EventArgs e)
        {
            //Process.Start("C:\\");

            String submittedText;
            if (!string.IsNullOrWhiteSpace(tb_Submit.Text))
            {
                submittedText = tb_Submit.Text;

                String[] split = submittedText.Split('(');
                command = split[0];
                int pFrom = submittedText.IndexOf("(") + "(".Length;
                int pTo = submittedText.LastIndexOf(")");
                if (pTo - pFrom > pFrom)
                {
                    String tempArgs = submittedText.Substring(pFrom, pTo - pFrom);
                    args = tempArgs.Split(',');
                }
                else
                {
                    tb_Output.AppendText("Wrong command format. Must be something like command(args..)\r\n");
                    return;
                }

                if (command.Equals("Submit", StringComparison.InvariantCultureIgnoreCase))
                {
                    //if (args.Length == 6)
                    // //   Submit(args[0], args[1], args[2], Int32.Parse(args[3]), args[4], args[5]);
                    //else
                    //    tb_Output.AppendText("Wrong number of args. Submit command must have 6 arguments");
                }
                else if (command.Equals("Worker", StringComparison.InvariantCultureIgnoreCase))
                {
                }
                else if (command.Equals("Wait", StringComparison.InvariantCultureIgnoreCase))
                {
                }
                else if (command.Equals("Status", StringComparison.InvariantCultureIgnoreCase))
                {
                }
                else if (command.Equals("SlowW", StringComparison.InvariantCultureIgnoreCase))
                {
                }
                else if (command.Equals("FreezeW", StringComparison.InvariantCultureIgnoreCase))
                {
                }
                else if (command.Equals("UnfreezeW", StringComparison.InvariantCultureIgnoreCase))
                {
                }
                else if (command.Equals("FreezeC", StringComparison.InvariantCultureIgnoreCase))
                {
                }
                else if (command.Equals("UnfreezeC", StringComparison.InvariantCultureIgnoreCase))
                {
                }
                else
                {
                    tb_Output.AppendText("Invalid command\r\n");
                    return;
                }
            }
            else
                tb_Output.AppendText("Please enter a command\r\n");
        }

    }

    public class PuppetMasterServices : MarshalByRefObject, IPuppetMaster { }
}
