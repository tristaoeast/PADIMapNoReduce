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
        private String args;

        public PuppetMasterGUI()
        {
            InitializeComponent();
        }

        private void Init(String EntryURL)
        {

        }

        private void bt_submit_Click(object sender, EventArgs e)
        {
            Process.Start("C:\\");

            String submittedText;
            if (!string.IsNullOrWhiteSpace(tb_Submit.Text))
            {
                submittedText = tb_Submit.Text;

                String[] split = submittedText.Split('(');
                command = split[0];
                int pFrom = submittedText.IndexOf("(") + "(".Length;
                int pTo = submittedText.LastIndexOf(")");
                if (pTo - pFrom > pFrom)
                    args = submittedText.Substring(pFrom, pTo - pFrom);
                else
                {
                    tb_Output.AppendText("Wrong command format. Must be something like command(args..)\r\n");
                    return;
                }

                if (command.Equals("Submit", StringComparison.InvariantCultureIgnoreCase))
                {
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
}
