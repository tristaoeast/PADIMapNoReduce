using MapLib;
using PuppetAppLib;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.IO;
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

        private void processCommand(String submText)
        {
            String[] split = submText.Split(null);
            command = split[0];

            if (command.Equals("Submit", StringComparison.InvariantCultureIgnoreCase))
            {
                if (split.Length == 7)
                    Submit(split[1], split[2], split[3], Int32.Parse(split[4]), split[5], File.ReadAllBytes(split[6]));
                else
                    tb_Output.AppendText("Wrong number of args. Submit command must have 6 arguments");
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

        private void Submit(String entryUrl, String inputFile, String outputDir, Int32 splits, String mapClassName, byte[] dll)
        {
            IAppPuppet app = (IAppPuppet)Activator.GetObject(typeof(IAppPuppet), "tcp://localhost:40001/U");
            app.Submit(entryUrl, inputFile, outputDir, splits, mapClassName, dll);
        }

        private void bt_loadScript_Click(object send, EventArgs e)
        {
            String pathToScript = tb_loadScript.Text;
            String line;

            System.IO.StreamReader file = new System.IO.StreamReader(pathToScript);
            while ((line = file.ReadLine()) != null)
            {
                processCommand(line);
            }
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

    public class PuppetMasterServices : MarshalByRefObject { }
}
