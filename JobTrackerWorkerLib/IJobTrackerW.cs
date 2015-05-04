using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JobTrackerWorkerLib
{
    public interface IJobTrackerW
    {
        IList<int> getSplitRange();
        void submitJob(long fileSize, int splits, String className, byte[] code);
    }
}
