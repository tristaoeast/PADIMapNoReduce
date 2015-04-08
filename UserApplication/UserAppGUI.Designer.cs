namespace UserApplication
{
    partial class UserAppGUI
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
            this.bt_init = new System.Windows.Forms.Button();
            this.lbl_url_cli = new System.Windows.Forms.Label();
            this.tb_url_cli = new System.Windows.Forms.TextBox();
            this.SuspendLayout();
            // 
            // bt_init
            // 
            this.bt_init.Location = new System.Drawing.Point(319, 17);
            this.bt_init.Name = "bt_init";
            this.bt_init.Size = new System.Drawing.Size(75, 23);
            this.bt_init.TabIndex = 0;
            this.bt_init.Text = "INIT";
            this.bt_init.UseVisualStyleBackColor = true;
            this.bt_init.Click += new System.EventHandler(this.bt_init_Click);
            // 
            // lbl_url_cli
            // 
            this.lbl_url_cli.AutoSize = true;
            this.lbl_url_cli.Location = new System.Drawing.Point(12, 22);
            this.lbl_url_cli.Name = "lbl_url_cli";
            this.lbl_url_cli.Size = new System.Drawing.Size(53, 13);
            this.lbl_url_cli.TabIndex = 1;
            this.lbl_url_cli.Text = "EntryURL";
            // 
            // tb_url_cli
            // 
            this.tb_url_cli.Location = new System.Drawing.Point(71, 19);
            this.tb_url_cli.Name = "tb_url_cli";
            this.tb_url_cli.Size = new System.Drawing.Size(225, 20);
            this.tb_url_cli.TabIndex = 2;
            // 
            // UserAppGUI
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(404, 90);
            this.Controls.Add(this.tb_url_cli);
            this.Controls.Add(this.lbl_url_cli);
            this.Controls.Add(this.bt_init);
            this.Name = "UserAppGUI";
            this.Text = "Form1";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button bt_init;
        private System.Windows.Forms.Label lbl_url_cli;
        private System.Windows.Forms.TextBox tb_url_cli;
    }
}

