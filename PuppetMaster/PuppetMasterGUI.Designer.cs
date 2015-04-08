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
            this.tb_submit = new System.Windows.Forms.TextBox();
            this.lbl_submit = new System.Windows.Forms.Label();
            this.bt_submit = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // tb_submit
            // 
            this.tb_submit.Location = new System.Drawing.Point(109, 9);
            this.tb_submit.Name = "tb_submit";
            this.tb_submit.Size = new System.Drawing.Size(691, 20);
            this.tb_submit.TabIndex = 0;
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
            // bt_submit
            // 
            this.bt_submit.Location = new System.Drawing.Point(806, 6);
            this.bt_submit.Name = "bt_submit";
            this.bt_submit.Size = new System.Drawing.Size(75, 23);
            this.bt_submit.TabIndex = 2;
            this.bt_submit.Text = "Submit";
            this.bt_submit.UseVisualStyleBackColor = true;
            this.bt_submit.Click += new System.EventHandler(this.Submit);
            // 
            // PuppetMasterGUI
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(911, 304);
            this.Controls.Add(this.bt_submit);
            this.Controls.Add(this.lbl_submit);
            this.Controls.Add(this.tb_submit);
            this.Name = "PuppetMasterGUI";
            this.Text = "Form1";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox tb_submit;
        private System.Windows.Forms.Label lbl_submit;
        private System.Windows.Forms.Button bt_submit;
    }
}

