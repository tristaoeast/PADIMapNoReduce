namespace PuppetMaster
{
    partial class PuppetMasterGUI
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.tb_Submit = new System.Windows.Forms.TextBox();
            this.lbl_submit = new System.Windows.Forms.Label();
            this.bt_Submit = new System.Windows.Forms.Button();
            this.tb_Output = new System.Windows.Forms.TextBox();
            this.lbl_loadScript = new System.Windows.Forms.Label();
            this.tb_loadScript = new System.Windows.Forms.TextBox();
            this.bt_loadScript = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // tb_Submit
            // 
            this.tb_Submit.Location = new System.Drawing.Point(109, 9);
            this.tb_Submit.Name = "tb_Submit";
            this.tb_Submit.Size = new System.Drawing.Size(691, 20);
            this.tb_Submit.TabIndex = 0;
            // 
            // lbl_submit
            // 
            this.lbl_submit.AutoSize = true;
            this.lbl_submit.Location = new System.Drawing.Point(7, 16);
            this.lbl_submit.Name = "lbl_submit";
            this.lbl_submit.Size = new System.Drawing.Size(89, 13);
            this.lbl_submit.TabIndex = 1;
            this.lbl_submit.Text = "Command Submit";
            this.lbl_submit.UseMnemonic = false;
            // 
            // bt_Submit
            // 
            this.bt_Submit.Location = new System.Drawing.Point(806, 6);
            this.bt_Submit.Name = "bt_Submit";
            this.bt_Submit.Size = new System.Drawing.Size(75, 23);
            this.bt_Submit.TabIndex = 2;
            this.bt_Submit.Text = "Submit";
            this.bt_Submit.UseVisualStyleBackColor = true;
            this.bt_Submit.Click += new System.EventHandler(this.bt_submit_Click);
            // 
            // tb_Output
            // 
            this.tb_Output.Location = new System.Drawing.Point(109, 73);
            this.tb_Output.Multiline = true;
            this.tb_Output.Name = "tb_Output";
            this.tb_Output.Size = new System.Drawing.Size(691, 195);
            this.tb_Output.TabIndex = 3;
            // 
            // lbl_loadScript
            // 
            this.lbl_loadScript.AutoSize = true;
            this.lbl_loadScript.Location = new System.Drawing.Point(7, 43);
            this.lbl_loadScript.Name = "lbl_loadScript";
            this.lbl_loadScript.Size = new System.Drawing.Size(71, 13);
            this.lbl_loadScript.TabIndex = 4;
            this.lbl_loadScript.Text = "Path to Script";
            this.lbl_loadScript.UseMnemonic = false;
            // 
            // tb_loadScript
            // 
            this.tb_loadScript.Location = new System.Drawing.Point(109, 40);
            this.tb_loadScript.Name = "tb_loadScript";
            this.tb_loadScript.Size = new System.Drawing.Size(691, 20);
            this.tb_loadScript.TabIndex = 5;
            // 
            // bt_loadScript
            // 
            this.bt_loadScript.Location = new System.Drawing.Point(806, 37);
            this.bt_loadScript.Name = "bt_loadScript";
            this.bt_loadScript.Size = new System.Drawing.Size(75, 23);
            this.bt_loadScript.TabIndex = 6;
            this.bt_loadScript.Text = "Load";
            this.bt_loadScript.UseVisualStyleBackColor = true;
            // 
            // PuppetMasterGUI
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(911, 304);
            this.Controls.Add(this.bt_loadScript);
            this.Controls.Add(this.tb_loadScript);
            this.Controls.Add(this.lbl_loadScript);
            this.Controls.Add(this.tb_Output);
            this.Controls.Add(this.bt_Submit);
            this.Controls.Add(this.lbl_submit);
            this.Controls.Add(this.tb_Submit);
            this.Name = "PuppetMasterGUI";
            this.Text = "Form1";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox tb_Submit;
        private System.Windows.Forms.Label lbl_submit;
        private System.Windows.Forms.Button bt_Submit;
        private System.Windows.Forms.TextBox tb_Output;
        private System.Windows.Forms.Label lbl_loadScript;
        private System.Windows.Forms.TextBox tb_loadScript;
        private System.Windows.Forms.Button bt_loadScript;
    }
}

