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
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace PuppetMaster
{
    public partial class PuppetMasterGUI : Form
    {
        private String command;

        public PuppetMasterGUI()
        {
            InitializeComponent();
        }

        private void processCommand(String submText)
        {
            //tb_Output.AppendText("Entering command processing...\r\n");
            String[] split = submText.Split(null);
            command = split[0];
            //tb_Output.AppendText("Command: "+ command +"\r\n");
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
                tb_Output.AppendText("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
            }
            else if (command.Equals("Wait", StringComparison.InvariantCultureIgnoreCase))
            {
                tb_Output.AppendText("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
            }
            else if (command.Equals("Status", StringComparison.InvariantCultureIgnoreCase))
            {
                tb_Output.AppendText("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
            }
            else if (command.Equals("SlowW", StringComparison.InvariantCultureIgnoreCase))
            {
                tb_Output.AppendText("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
            }
            else if (command.Equals("FreezeW", StringComparison.InvariantCultureIgnoreCase))
            {
                tb_Output.AppendText("Command: " + command + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5] + " " + split[6]);
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

        private void Submit(String entryUrl, String inputFile, String outputDir, Int32 splits, String mapClassName, byte[] dll)
        {
            IApp app = (IApp)Activator.GetObject(typeof(IApp), "tcp://localhost:40001/U");
            app.Submit(entryUrl, inputFile, outputDir, splits, mapClassName, dll);
        }

        private void bt_loadScript_Click(object sender, EventArgs e)
        {
            String pathToScript;
            tb_Output.AppendText("Load button pressed...\r\n");

            if (!string.IsNullOrWhiteSpace(tb_loadScript.Text))
            {
                pathToScript = tb_loadScript.Text;
                String line;

                System.IO.StreamReader file = new System.IO.StreamReader(pathToScript);
                while ((line = file.ReadLine()) != null)
                {
                    tb_Output.AppendText("line: " + line + "\r\n");
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

    public class PuppetMasterServices : MarshalByRefObject, IPuppetMaster { }
}
